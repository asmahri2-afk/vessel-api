import json
import re
import requests
import asyncio
import websockets
import logging
import os
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ============================================================
# LOGGING CONFIG
# ============================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# HTTP CONFIG
# ============================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.vesselfinder.com/",
}

MYSHIPTRACKING_URL = "https://www.myshiptracking.com/requests/vesselsonmaptempTTT.php"

# ============================================================
# AISSTREAM CONFIG (via environnement)
# ============================================================
AISSTREAM_API_KEY = os.environ.get("AISSTREAM_API_KEY", "928b33be84745728566f4d4c9628386b0989eca3")  # À remplacer par votre clé

# ============================================================
# FASTAPI APP + CORS
# ============================================================

app = FastAPI()

@app.get("/ping")
def ping():
    return {"ok": True}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_cors_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

# ============================================================
# UTILITY HELPERS
# ============================================================

def count_decimals(val: Any) -> int:
    """Counts decimal places to determine coordinate precision."""
    if val is None:
        return 0
    s = str(val)
    if "." in s:
        return len(s.split(".")[-1].rstrip("0"))
    return 0

def get_vf_age_minutes(age_str: Optional[str]) -> int:
    """Parses VesselFinder age strings like '3 min ago' or '2 hours ago'."""
    if not age_str:
        return 999
    age_str = age_str.lower()
    if "now" in age_str or "just" in age_str:
        return 0
    match = re.search(r"(\d+)", age_str)
    if not match:
        return 999
    value = int(match.group(1))
    if "hour" in age_str:
        return value * 60
    if "day" in age_str:
        return value * 1440
    return value

def is_valid_coordinates(lat: Optional[float], lon: Optional[float]) -> bool:
    """Vérifie que les coordonnées sont plausibles."""
    if lat is None or lon is None:
        return False
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0

# ============================================================
# HTML HELPERS – VESSELFINDER
# ============================================================

def extract_table_data(soup: BeautifulSoup, table_class: str) -> Dict[str, str]:
    data: Dict[str, str] = {}
    tables = soup.find_all(class_=table_class)
    if not tables:
        return data

    for table in tables:
        for row in table.find_all("tr"):
            label_el = row.find(
                class_=lambda x: x and ("tpc1" in x or "tpx1" in x or "n3" in x)
            )
            value_el = row.find(
                class_=lambda x: x and ("tpc2" in x or "tpx2" in x or "v3" in x)
            )
            if not (label_el and value_el):
                continue
            label_parts = [c.strip() for c in label_el.contents if isinstance(c, str)]
            label = " ".join(label_parts).replace(":", "").strip()
            value = value_el.get_text(strip=True)
            if label:
                data[label] = value
    return data

def extract_mmsi(soup: BeautifulSoup, static_data: Dict[str, str]) -> Optional[str]:
    for s in soup.find_all("script"):
        if not s.string: continue
        m = re.search(r"MMSI\s*=\s*(\d+)", s.string)
        if m: return m.group(1)
    if "MMSI" in static_data:
        v = static_data["MMSI"].strip()
        if v: return v
    for key, value in static_data.items():
        if "MMSI" in key.upper():
            v = value.strip()
            if v: return v
    return None

# ============================================================
# MYSHIPTRACKING HELPER
# ============================================================

def get_myshiptracking_pos(
    mmsi: str,
    center_lat: Optional[float],
    center_lon: Optional[float],
    pad: float = 0.9,
) -> Optional[Dict[str, Any]]:
    if center_lat is None or center_lon is None:
        return None
    try:
        lat_f, lon_f = float(center_lat), float(center_lon)
    except (TypeError, ValueError):
        return None

    params = {
        "type": "json",
        "minlat": lat_f - pad, "maxlat": lat_f + pad,
        "minlon": lon_f - pad, "maxlon": lon_f + pad,
        "zoom": 15, "selid": -1, "seltype": 0, "timecode": -1,
        "filters": json.dumps({
            "vtypes": ",0,3,4,6,7,8,9,10,11,12,13", "ports": "1",
            "minsog": 0, "maxsog": 60, "minsz": 0, "maxsz": 500,
            "minyr": 1950, "maxyr": 2025, "status": "",
            "mapflt_from": "", "mapflt_dest": "",
        }),
    }
    mst_headers = dict(HEADERS)
    mst_headers["Referer"] = "https://www.myshiptracking.com/"

    try:
        r = requests.get(MYSHIPTRACKING_URL, params=params, headers=mst_headers, timeout=10)
        if r.status_code != 200: return None
        lines = [l.strip() for l in r.text.splitlines() if l.strip()]
        if len(lines) < 3: return None
        target_mmsi = str(mmsi).strip()
        for line in lines[2:]:
            parts = line.split("\t") if "\t" in line else line.split()
            if len(parts) >= 7 and parts[2].strip() == target_mmsi:
                return {
                    "lat": float(parts[4]), "lon": float(parts[5]),
                    "sog": float(parts[6]) if parts[6] != "" else None,
                    "cog": float(parts[7]) if len(parts) > 7 and parts[7] != "" else None,
                }
    except Exception as e:
        logger.error(f"MyShipTracking error for MMSI {mmsi}: {e}")
    return None

# ============================================================
# AISSTREAM HELPER (temps réel)
# ============================================================

@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=2, max=5),
    retry=retry_if_exception_type((websockets.WebSocketException, asyncio.TimeoutError, ConnectionError)),
    before_sleep=lambda retry_state: logger.warning(f"Retrying AISStream (attempt {retry_state.attempt_number})...")
)
def get_aisstream_pos(mmsi: str) -> Optional[Dict[str, Any]]:
    """Récupère la dernière position via AISStream.io (WebSocket)."""
    if not mmsi or not AISSTREAM_API_KEY:
        logger.warning("AISStream: MMSI ou API key manquant")
        return None

    async def fetch():
        try:
            async with asyncio.timeout(20):
                async with websockets.connect("wss://stream.aisstream.io/v0/stream", ping_interval=None) as websocket:
                    subscribe_message = {
                        "APIKey": AISSTREAM_API_KEY,
                        "BoundingBoxes": [[[-90, -180], [90, 180]]],
                        "FiltersShipMMSI": [str(mmsi)],
                        "FilterMessageTypes": ["PositionReport"]
                    }
                    await websocket.send(json.dumps(subscribe_message))
                    logger.debug(f"AISStream subscription sent for MMSI {mmsi}")

                    try:
                        message_json = await asyncio.wait_for(websocket.recv(), timeout=15.0)
                    except asyncio.TimeoutError:
                        logger.info(f"AISStream: timeout waiting for message for MMSI {mmsi}")
                        return None

                    message = json.loads(message_json)
                    if "error" in message:
                        logger.error(f"AISStream error for MMSI {mmsi}: {message['error']}")
                        return None

                    if message.get("MessageType") == "PositionReport":
                        ais_message = message['Message']['PositionReport']
                        return {
                            "lat": float(ais_message['Latitude']),
                            "lon": float(ais_message['Longitude']),
                            "sog": float(ais_message.get('Sog')) if ais_message.get('Sog') is not None else None,
                            "cog": float(ais_message.get('Cog')) if ais_message.get('Cog') is not None else None,
                        }
                    else:
                        logger.debug(f"AISStream: received non-position message: {message.get('MessageType')}")
                        return None
        except Exception as e:
            logger.error(f"AISStream exception for MMSI {mmsi}: {e}")
            raise

    # Gestion de la boucle asyncio
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(fetch())

# ============================================================
# MAIN SCRAPER – SMART MERGE LOGIC (avec AISStream)
# ============================================================

def scrape_vf_full(imo: str) -> Dict[str, Any]:
    url = f"https://www.vesselfinder.com/vessels/details/{imo}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
    except requests.RequestException as e:
        logger.error(f"VesselFinder request failed for IMO {imo}: {e}")
        return {"found": False, "imo": imo, "error": str(e)}

    if r.status_code == 404:
        logger.info(f"VesselFinder: IMO {imo} not found")
        return {"found": False, "imo": imo}
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # --- Basic Info ---
    name_el = soup.select_one("h1.title")
    name = name_el.get_text(strip=True) if name_el else f"IMO {imo}"
    dest_el = soup.select_one("div.vi__r1.vi__sbt a._npNa")
    destination = dest_el.get_text(strip=True) if dest_el else ""

    info_icon = soup.select_one("svg.ttt1.info")
    last_pos_utc = info_icon["data-title"] if info_icon and info_icon.has_attr("data-title") else None

    # --- Static Data ---
    tech_data = extract_table_data(soup, "tpt1")
    dims_data = extract_table_data(soup, "tptfix")
    ais_table_data = extract_table_data(soup, "vessel-info-table") 
    aparams_data = extract_table_data(soup, "aparams")
    
    static_data = {**tech_data, **dims_data, **ais_table_data, **aparams_data}
    mmsi = extract_mmsi(soup, static_data)

    # Draught
    draught_val = static_data.get("Current draught") or static_data.get("Draught")
    if not draught_val:
        match = re.search(r"(?:draught|draft)\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*m", soup.get_text(), re.IGNORECASE)
        if match: draught_val = f"{match.group(1)} m"

    final_static_data = {
        "imo": imo, "vessel_name": name, "ship_type": static_data.get("Ship type") or "",
        "flag": (soup.select_one("div.title-flag-icon").get("title") if soup.select_one("div.title-flag-icon") else None),
        "mmsi": mmsi, "draught_m": draught_val or "",
        "deadweight_t": static_data.get("Deadweight") or static_data.get("DWT"),
        "gross_tonnage": static_data.get("Gross Tonnage"),
        "year_of_build": static_data.get("Year of Build"),
        "length_overall_m": static_data.get("Length Overall"),
        "beam_m": static_data.get("Beam"),
    }

    # --- VesselFinder AIS Extraction ---
    vf_lat = vf_lon = sog = cog = None
    djson_div = soup.find("div", id="djson")
    if djson_div and djson_div.has_attr("data-json"):
        try:
            ais = json.loads(djson_div["data-json"])
            vf_lat = float(ais.get("ship_lat")) if ais.get("ship_lat") else None
            vf_lon = float(ais.get("ship_lon")) if ais.get("ship_lon") else None
            sog, cog = ais.get("ship_sog"), ais.get("ship_cog")
        except Exception as e:
            logger.warning(f"VesselFinder: failed to parse djson for IMO {imo}: {e}")

    # --- Nouvelle source AISStream ---
    aisstream_data = None
    if mmsi and AISSTREAM_API_KEY:
        try:
            aisstream_data = get_aisstream_pos(mmsi)
            logger.info(f"aisstream_data for IMO {imo}: {aisstream_data}")
        except Exception as e:
            logger.error(f"AISStream failed for MMSI {mmsi}: {e}")

    # --- MyShipTracking ---
    mst_data = None
    if mmsi and vf_lat:
        try:
            mst_data = get_myshiptracking_pos(mmsi, vf_lat, vf_lon)
            logger.info(f"mst_data for IMO {imo}: {mst_data}")
        except Exception as e:
            logger.error(f"MyShipTracking failed for MMSI {mmsi}: {e}")

    vf_age = get_vf_age_minutes(last_pos_utc)

    # --- SMART MERGE avec priorité AISStream ---
    selected = None

    # 1. AISStream (données temps réel)
    if aisstream_data and is_valid_coordinates(aisstream_data.get("lat"), aisstream_data.get("lon")):
        selected = ("aisstream", aisstream_data)

    # 2. MyShipTracking (si VF est vieux ou absent)
    elif mst_data and is_valid_coordinates(mst_data.get("lat"), mst_data.get("lon")) and (vf_lat is None or vf_age > 60):
        selected = ("myshiptracking", mst_data)

    # 3. VesselFinder (fallback)
    elif vf_lat is not None and vf_lon is not None and is_valid_coordinates(vf_lat, vf_lon):
        selected = ("vesselfinder", {"lat": vf_lat, "lon": vf_lon, "sog": sog, "cog": cog})

    if selected:
        ais_source, data = selected
        lat, lon = data["lat"], data["lon"]
        sog = data.get("sog", sog)
        cog = data.get("cog", cog)
        logger.info(f"IMO {imo}: using {ais_source} position: {lat}, {lon}")
    else:
        lat = lon = sog = cog = None
        ais_source = "none"
        logger.warning(f"IMO {imo}: no valid position source")

    return {
        "found": True,
        "destination": destination,
        "last_pos_utc": last_pos_utc,
        **final_static_data,
        "lat": lat, "lon": lon, "sog": sog, "cog": cog,
        "ais_source": ais_source,
    }

# ============================================================
# API ENDPOINT
# ============================================================

@app.get("/vessel-full/{imo}")
def vessel_full(imo: str):
    data = scrape_vf_full(imo)
    if not data.get("found"):
        raise HTTPException(status_code=404, detail="Vessel not found")
    return data

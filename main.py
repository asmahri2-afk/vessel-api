import json
import re
import random
import time
import logging
from typing import Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import curl_cffi.requests as curl_requests

# ============================================================
# LOGGING CONFIG
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
    """Counts decimal places to determine coordinate precisions."""
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
# EXTERNAL SOURCE HELPERS (with retries and logging)
# ============================================================

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.RequestException, curl_requests.RequestsError)),
    before_sleep=lambda retry_state: logger.warning(f"Retrying MarineTraffic (attempt {retry_state.attempt_number})...")
)
def get_marinetraffic_pos(imo: str) -> Optional[Dict[str, Any]]:
    """
    Récupère la position depuis MarineTraffic avec curl_cffi pour imiter le TLS d'un vrai navigateur.
    Inclut retries et logging.
    """
    # Headers ultra-complets (identiques à un vrai Chrome)
    mt_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    # Utilisation d'une session curl_cffi pour conserver les cookies
    session = curl_requests.Session()
    session.headers.update(mt_headers)

    try:
        # Étape 1 : page d'accueil pour obtenir les cookies
        home_url = "https://www.marinetraffic.com/"
        logger.info(f"MarineTraffic: fetching homepage for IMO {imo}")
        session.get(home_url, timeout=15, impersonate="chrome122")

        # Délai aléatoire (simule navigation humaine)
        time.sleep(random.uniform(1, 3))

        # Étape 2 : page du navire (redirection vers shipid)
        base_url = f"https://www.marinetraffic.com/en/ais/details/ships/imo:{imo}"
        logger.info(f"MarineTraffic: fetching vessel page for IMO {imo}")
        r_info = session.get(base_url, timeout=15, allow_redirects=True, impersonate="chrome122")

        # Extraire shipid de l'URL finale
        match = re.search(r"shipid:(\d+)", r_info.url)
        if not match:
            logger.warning(f"MarineTraffic: shipid not found for IMO {imo}")
            return None
        shipid = match.group(1)
        logger.info(f"MarineTraffic: found shipid {shipid} for IMO {imo}")

        # Étape 3 : appel AJAX pour la position
        pos_url = f"https://www.marinetraffic.com/en/vessels/{shipid}/position"
        pos_headers = dict(mt_headers)
        pos_headers["X-Requested-With"] = "XMLHttpRequest"
        pos_headers["Accept"] = "application/json"

        r_pos = session.get(pos_url, headers=pos_headers, timeout=15, impersonate="chrome122")
        if r_pos.status_code == 200:
            d = r_pos.json()
            logger.info(f"MarineTraffic: success for IMO {imo}")
            return {
                "lat": float(d.get("lat")),
                "lon": float(d.get("lon")),
                "sog": float(d.get("speed")) if d.get("speed") is not None else None,
                "cog": float(d.get("course")) if d.get("course") is not None else None
            }
        else:
            logger.warning(f"MarineTraffic: position endpoint returned {r_pos.status_code} for IMO {imo}")
            return None
    except Exception as e:
        logger.error(f"MarineTraffic: exception for IMO {imo}: {e}")
        raise  # pour que tenacity retry

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.RequestException),
    before_sleep=lambda retry_state: logger.warning(f"Retrying MyShipTracking (attempt {retry_state.attempt_number})...")
)
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
        logger.warning(f"MyShipTracking: invalid center coordinates for MMSI {mmsi}")
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
        logger.info(f"MyShipTracking: querying for MMSI {mmsi}")
        r = requests.get(MYSHIPTRACKING_URL, params=params, headers=mst_headers, timeout=10)
        if r.status_code != 200:
            logger.warning(f"MyShipTracking: returned {r.status_code} for MMSI {mmsi}")
            return None
        lines = [l.strip() for l in r.text.splitlines() if l.strip()]
        if len(lines) < 3:
            logger.debug(f"MyShipTracking: insufficient lines for MMSI {mmsi}")
            return None

        target_mmsi = str(mmsi).strip()
        for line in lines[2:]:
            parts = line.split("\t") if "\t" in line else line.split()
            if len(parts) >= 7 and parts[2].strip() == target_mmsi:
                logger.info(f"MyShipTracking: found position for MMSI {mmsi}")
                return {
                    "lat": float(parts[4]), "lon": float(parts[5]),
                    "sog": float(parts[6]) if parts[6] != "" else None,
                    "cog": float(parts[7]) if len(parts) > 7 and parts[7] != "" else None,
                }
        logger.debug(f"MyShipTracking: MMSI {mmsi} not found in response")
        return None
    except Exception as e:
        logger.error(f"MyShipTracking: exception for MMSI {mmsi}: {e}")
        raise  # pour retry

# ============================================================
# MAIN SCRAPER – SMART MERGE LOGIC
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
    if r.status_code != 200:
        logger.warning(f"VesselFinder returned {r.status_code} for IMO {imo}")
        return {"found": False, "imo": imo, "error": f"HTTP {r.status_code}"}

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

    # --- SMART MERGE LOGIC (with MarineTraffic) ---
    mt_data = None
    try:
        mt_data = get_marinetraffic_pos(imo)
    except Exception as e:
        logger.error(f"All MarineTraffic retries failed for IMO {imo}: {e}")

    mst_data = None
    if mmsi and vf_lat:
        try:
            mst_data = get_myshiptracking_pos(mmsi, vf_lat, vf_lon)
        except Exception as e:
            logger.error(f"All MyShipTracking retries failed for MMSI {mmsi}: {e}")
    
    vf_age = get_vf_age_minutes(last_pos_utc)
    
    # Priority 1: MarineTraffic
    if mt_data:
        lat, lon = mt_data["lat"], mt_data["lon"]
        sog = mt_data.get("sog", sog)
        cog = mt_data.get("cog", cog)
        ais_source = "marinetraffic"
        logger.info(f"IMO {imo}: using MarineTraffic position")
    
    # Priority 2: MyShipTracking (if VF old or missing)
    elif mst_data and (vf_lat is None or vf_age > 60):
        lat, lon = mst_data["lat"], mst_data["lon"]
        sog = mst_data.get("sog", sog)
        cog = mst_data.get("cog", cog)
        ais_source = "myshiptracking"
        logger.info(f"IMO {imo}: using MyShipTracking position")
        
    # Priority 3: VesselFinder
    else:
        lat, lon = vf_lat, vf_lon
        ais_source = "vesselfinder"
        logger.info(f"IMO {imo}: using VesselFinder position")

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

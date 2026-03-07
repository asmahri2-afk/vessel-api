import json
import logging
import os
import re
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, Optional

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
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

# FIX #5: Simple API key auth — set this as an env var on Render
API_SECRET = os.getenv("API_SECRET", "")

# ============================================================
# FASTAPI APP + CORS
# ============================================================

app = FastAPI()

# FIX #2: Single CORS middleware only — removed duplicate custom middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

# FIX #1: Replaced broken get_vf_age_minutes() with real timestamp parser
def parse_vf_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parses VesselFinder timestamps like 'Mar 07, 2026 00:05 UTC'."""
    if not ts:
        return None
    ts = ts.replace(" UTC", "").strip()
    for fmt in ("%b %d, %Y %H:%M", "%B %d, %Y %H:%M"):
        try:
            return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    logger.warning(f"Could not parse VF timestamp: '{ts}'")
    return None

def get_vf_age_minutes(last_pos_utc: Optional[str]) -> int:
    """Returns age of VesselFinder position in minutes. Returns 999 if unparseable."""
    dt = parse_vf_timestamp(last_pos_utc)
    if not dt:
        return 999
    age = (datetime.now(timezone.utc) - dt).total_seconds() / 60
    return int(age)

# ============================================================
# INPUT VALIDATION
# ============================================================

# FIX #3: Validate IMO before making any external requests
def validate_imo(imo: str) -> bool:
    imo = str(imo).strip()
    if not re.match(r'^\d{7}$', imo):
        return False
    try:
        total = sum(int(imo[i]) * (7 - i) for i in range(6))
        return int(imo[6]) == total % 10
    except Exception:
        return False

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
        if not s.string:
            continue
        m = re.search(r"MMSI\s*=\s*(\d+)", s.string)
        if m:
            return m.group(1)
    if "MMSI" in static_data:
        v = static_data["MMSI"].strip()
        if v:
            return v
    for key, value in static_data.items():
        if "MMSI" in key.upper():
            v = value.strip()
            if v:
                return v
    return None

# ============================================================
# MYSHIPTRACKING HELPER
# ============================================================

def get_myshiptracking_pos(
    mmsi: str,
    center_lat: Optional[float],
    center_lon: Optional[float],
    session: requests.Session,
    pad: float = 0.9,
) -> Optional[Dict[str, Any]]:
    if center_lat is None or center_lon is None:
        return None

    try:
        lat_f, lon_f = float(center_lat), float(center_lon)
    except (TypeError, ValueError):
        return None

    # FIX #9: Use current year dynamically instead of hardcoded 2025
    current_year = datetime.now().year

    params = {
        "type": "json",
        "minlat": lat_f - pad, "maxlat": lat_f + pad,
        "minlon": lon_f - pad, "maxlon": lon_f + pad,
        "zoom": 15, "selid": -1, "seltype": 0, "timecode": -1,
        "filters": json.dumps({
            "vtypes": ",0,3,4,6,7,8,9,10,11,12,13", "ports": "1",
            "minsog": 0, "maxsog": 60, "minsz": 0, "maxsz": 500,
            "minyr": 1950, "maxyr": current_year, "status": "",
            "mapflt_from": "", "mapflt_dest": "",
        }),
    }

    mst_headers = dict(HEADERS)
    mst_headers["Referer"] = "https://www.myshiptracking.com/"

    try:
        # FIX #6: Use session for connection reuse
        r = session.get(MYSHIPTRACKING_URL, params=params, headers=mst_headers, timeout=10)
        if r.status_code != 200:
            logger.warning(f"MyShipTracking returned status {r.status_code} for MMSI {mmsi}")
            return None

        lines = [l.strip() for l in r.text.splitlines() if l.strip()]
        if len(lines) < 3:
            return None

        target_mmsi = str(mmsi).strip()
        for line in lines[2:]:
            parts = line.split("\t") if "\t" in line else line.split()
            if len(parts) >= 7 and parts[2].strip() == target_mmsi:
                return {
                    "lat": float(parts[4]),
                    "lon": float(parts[5]),
                    "sog": float(parts[6]) if parts[6] != "" else None,
                    "cog": float(parts[7]) if len(parts) > 7 and parts[7] != "" else None,
                }

    # FIX #7: Log MST failures instead of silently passing
    except Exception as e:
        logger.warning(f"MyShipTracking fetch failed for MMSI {mmsi}: {e}")

    return None

# ============================================================
# MAIN SCRAPER – SMART MERGE LOGIC
# ============================================================

def scrape_vf_full(imo: str, session: requests.Session) -> Dict[str, Any]:
    url = f"https://www.vesselfinder.com/vessels/details/{imo}"

    # FIX #6: Use session for connection reuse
    r = session.get(url, headers=HEADERS, timeout=20)

    if r.status_code == 404:
        logger.info(f"IMO {imo} returned 404 from VesselFinder")
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
    logger.info(f"IMO {imo} | name={name} | last_pos_utc={last_pos_utc}")

    # --- Static Data ---
    tech_data      = extract_table_data(soup, "tpt1")
    dims_data      = extract_table_data(soup, "tptfix")
    ais_table_data = extract_table_data(soup, "vessel-info-table")
    aparams_data   = extract_table_data(soup, "aparams")
    static_data    = {**tech_data, **dims_data, **ais_table_data, **aparams_data}
    mmsi           = extract_mmsi(soup, static_data)

    # Draught
    draught_val = static_data.get("Current draught") or static_data.get("Draught")
    if not draught_val:
        match = re.search(r"(?:draught|draft)\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*m", soup.get_text(), re.IGNORECASE)
        if match:
            draught_val = f"{match.group(1)} m"

    final_static_data = {
        "imo": imo,
        "vessel_name": name,
        "ship_type": static_data.get("Ship Type") or static_data.get("Ship type") or static_data.get("Type") or "",
        "flag": (soup.select_one("div.title-flag-icon").get("title") if soup.select_one("div.title-flag-icon") else None),
        "mmsi": mmsi,
        "draught_m": draught_val or "",
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
        # FIX #4: Log AIS parse failures instead of silently passing
        try:
            ais = json.loads(djson_div["data-json"])
            vf_lat = float(ais.get("ship_lat")) if ais.get("ship_lat") else None
            vf_lon = float(ais.get("ship_lon")) if ais.get("ship_lon") else None
            sog = ais.get("ship_sog")
            cog = ais.get("ship_cog")
            logger.info(f"IMO {imo} | VF AIS: lat={vf_lat}, lon={vf_lon}, sog={sog}, cog={cog}")
        except Exception as e:
            logger.warning(f"IMO {imo} | Failed to parse djson AIS data: {e}")

    # --- SMART MERGE LOGIC ---
    mst_data = get_myshiptracking_pos(mmsi, vf_lat, vf_lon, session) if (mmsi and vf_lat) else None

    use_mst  = False
    # FIX #1: vf_age now correctly computed from real timestamp
    vf_age   = get_vf_age_minutes(last_pos_utc)
    MAX_VF_AGE = 60

    if mst_data:
        vf_precision  = count_decimals(vf_lat) + count_decimals(vf_lon) if vf_lat is not None else 0
        mst_precision = count_decimals(mst_data["lat"]) + count_decimals(mst_data["lon"])

        if vf_lat is None:
            use_mst = True
            logger.info(f"IMO {imo} | Using MST: VF has no position")
        elif vf_age > MAX_VF_AGE:
            use_mst = True
            logger.info(f"IMO {imo} | Using MST: VF data is {vf_age} min old (>{MAX_VF_AGE})")
        elif mst_precision > vf_precision:
            use_mst = True
            logger.info(f"IMO {imo} | Using MST: higher precision ({mst_precision} vs {vf_precision})")
        else:
            use_mst = False
            logger.info(f"IMO {imo} | Using VF: fresher or equal precision (age={vf_age} min)")

    if use_mst and mst_data:
        lat, lon = mst_data["lat"], mst_data["lon"]
        sog = mst_data.get("sog", sog)
        cog = mst_data.get("cog", cog)
        ais_source = "myshiptracking"
    else:
        lat, lon = vf_lat, vf_lon
        ais_source = "vesselfinder"

    logger.info(f"IMO {imo} | Final: lat={lat}, lon={lon}, sog={sog}, source={ais_source}")

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

@app.get("/ping")
def ping():
    return {"ok": True}

@app.get("/vessel-full/{imo}")
def vessel_full(imo: str, request: Request):
    # FIX #5: Simple API key check — header X-API-Secret must match env var
    if API_SECRET:
        client_secret = request.headers.get("X-API-Secret", "")
        if client_secret != API_SECRET:
            logger.warning(f"Unauthorized request for IMO {imo} from {request.client.host}")
            raise HTTPException(status_code=401, detail="Unauthorized")

    # FIX #3: Validate IMO format and checksum before hitting VesselFinder
    if not validate_imo(imo):
        logger.warning(f"Invalid IMO rejected: {imo}")
        raise HTTPException(status_code=400, detail="Invalid IMO number")

    # FIX #6: Session per request for connection reuse within one scrape
    with requests.Session() as session:
        try:
            data = scrape_vf_full(imo, session)
        except Exception as e:
            logger.error(f"Scrape failed for IMO {imo}: {e}", exc_info=True)
            raise HTTPException(status_code=502, detail="Upstream scrape failed")

    if not data.get("found"):
        raise HTTPException(status_code=404, detail="Vessel not found")

    return data

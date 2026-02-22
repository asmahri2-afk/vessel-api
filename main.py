"""
Vessel Tracker API - Production Ready Version
==============================================
Fetches vessel information and AIS position data from VesselFinder and MyShipTracking.
Ajoute un proxy WebSocket pour AISStream.

Usage:
    Set environment variables (optional):
    - LOG_LEVEL: Logging level (default: INFO)
    - AISSTREAM_API_KEY: Your AISStream API key
    
    Run: uvicorn vessel_tracker:app --host 0.0.0.0 --port 8000
"""

import json
import re
import logging
import os
from typing import Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Path, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import websockets

# ============================================================
# LOGGING CONFIG
# ============================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================
# HTTP CONFIG
# ============================================================
DEFAULT_TIMEOUT = 20
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

VESSELFINDER_BASE_URL = "https://www.vesselfinder.com/vessels/details"
MYSHIPTRACKING_URL = "https://www.myshiptracking.com/requests/vesselsonmaptempTTT.php"

# ============================================================
# FASTAPI APP + CORS
# ============================================================
app = FastAPI(
    title="Vessel Tracker API",
    description="Fetch vessel information and AIS position data",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this for production
    allow_credentials=False,
    allow_methods=["GET", "WEBSOCKET"],  # Ajout de WEBSOCKET pour les connexions WebSocket
    allow_headers=["*"],
)

# ============================================================
# WEBSOCKET PROXY FOR AISSTREAM
# ============================================================
AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
# Lire la clé depuis l'environnement, avec une valeur par défaut pour le test
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY", "928b33be84745728566f4d4c9628386b0989eca3")

@app.websocket("/ws/ais-stream")
async def websocket_ais_stream(websocket: WebSocket):
    await websocket.accept()
    print("Frontend client connected to proxy")

    try:
        # Connexion à AISStream
        async with websockets.connect(AISSTREAM_URL) as ais_ws:
            print("Connected to AISStream")

            # Envoyer la souscription
            subscription = {
                "APIKey": AISSTREAM_API_KEY,
                "BoundingBoxes": [[[-90, -180], [90, 180]]],
                "FilterMessageTypes": ["PositionReport"]
            }
            await ais_ws.send(json.dumps(subscription))
            print("Subscription sent to AISStream")

            # Relayer les messages
            async for message in ais_ws:
                # Vous pouvez filtrer ici si besoin
                await websocket.send_text(message)

    except websockets.exceptions.ConnectionClosed as e:
        print(f"AISStream connection closed: {e}")
    except WebSocketDisconnect:
        print("Frontend client disconnected")
    except Exception as e:
        print(f"Error in WebSocket proxy: {e}")
    finally:
        print("WebSocket proxy closed")

# ============================================================
# PYDANTIC MODELS
# ============================================================

class VesselResponse(BaseModel):
    """Response model for vessel data"""
    found: bool
    imo: str
    vessel_name: Optional[str] = None
    mmsi: Optional[str] = None
    ship_type: Optional[str] = None
    flag: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    sog: Optional[float] = None
    cog: Optional[float] = None
    destination: Optional[str] = None
    last_pos_utc: Optional[str] = None
    ais_source: str = "none"
    draught_m: Optional[str] = None
    deadweight_t: Optional[str] = None
    gross_tonnage: Optional[str] = None
    year_of_build: Optional[str] = None
    length_overall_m: Optional[str] = None
    beam_m: Optional[str] = None
    error: Optional[str] = None

class HealthResponse(BaseModel):
    """Health check response"""
    status: str = "healthy"
    version: str = "2.0.0"


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
    age_str = age_str.lower().strip()
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
    """Validates that coordinates are within plausible ranges."""
    if lat is None or lon is None:
        return False
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0


def validate_imo(imo: str) -> bool:
    """
    Validates IMO number format (7 digits with valid checksum).
    IMO checksum: each digit multiplied by position (7 to 1), sum mod 10 = 0
    """
    imo = imo.strip()
    if not imo.isdigit() or len(imo) != 7:
        return False
    
    # Calculate checksum
    digits = [int(d) for d in imo]
    checksum = sum(d * (7 - i) for i, d in enumerate(digits[:6])) % 10
    return checksum == digits[6]


# ============================================================
# HTML HELPERS – VESSELFINDER
# ============================================================

def extract_table_data(soup: BeautifulSoup, table_class: str) -> Dict[str, str]:
    """Extracts key-value pairs from VesselFinder data tables."""
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
    """Extracts MMSI from VesselFinder page (script tag or data tables)."""
    # Try to find in script tags
    for script in soup.find_all("script"):
        if not script.string:
            continue
        match = re.search(r"MMSI\s*=\s*(\d+)", script.string)
        if match:
            return match.group(1)
    
    # Try static data tables
    if "MMSI" in static_data:
        value = static_data["MMSI"].strip()
        if value:
            return value
    
    # Search all keys for MMSI
    for key, value in static_data.items():
        if "MMSI" in key.upper():
            value = value.strip()
            if value:
                return value
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
    """
    Fetches vessel position from MyShipTracking.
    Uses a bounding box around known position to find the vessel.
    """
    if center_lat is None or center_lon is None:
        return None
    
    try:
        lat_f, lon_f = float(center_lat), float(center_lon)
    except (TypeError, ValueError):
        return None

    params = {
        "type": "json",
        "minlat": lat_f - pad,
        "maxlat": lat_f + pad,
        "minlon": lon_f - pad,
        "maxlon": lon_f + pad,
        "zoom": 15,
        "selid": -1,
        "seltype": 0,
        "timecode": -1,
        "filters": json.dumps({
            "vtypes": ",0,3,4,6,7,8,9,10,11,12,13",
            "ports": "1",
            "minsog": 0,
            "maxsog": 60,
            "minsz": 0,
            "maxsz": 500,
            "minyr": 1950,
            "maxyr": 2025,
            "status": "",
            "mapflt_from": "",
            "mapflt_dest": "",
        }),
    }
    
    mst_headers = dict(HEADERS)
    mst_headers["Referer"] = "https://www.myshiptracking.com/"

    try:
        response = requests.get(
            MYSHIPTRACKING_URL,
            params=params,
            headers=mst_headers,
            timeout=10
        )
        
        if response.status_code != 200:
            logger.warning(f"MyShipTracking returned status {response.status_code}")
            return None
        
        lines = [line.strip() for line in response.text.splitlines() if line.strip()]
        if len(lines) < 3:
            return None
        
        target_mmsi = str(mmsi).strip()
        for line in lines[2:]:
            parts = line.split("\t") if "\t" in line else line.split()
            if len(parts) >= 7 and parts[2].strip() == target_mmsi:
                return {
                    "lat": float(parts[4]),
                    "lon": float(parts[5]),
                    "sog": float(parts[6]) if parts[6] else None,
                    "cog": float(parts[7]) if len(parts) > 7 and parts[7] else None,
                }
                
    except requests.Timeout:
        logger.warning(f"MyShipTracking timeout for MMSI {mmsi}")
    except requests.RequestException as e:
        logger.error(f"MyShipTracking request error for MMSI {mmsi}: {e}")
    except (ValueError, IndexError) as e:
        logger.error(f"MyShipTracking parse error for MMSI {mmsi}: {e}")
    
    return None


# ============================================================
# MAIN SCRAPER – VESSELFINDER
# ============================================================

def scrape_vesselfinder(imo: str) -> Dict[str, Any]:
    """
    Scrapes vessel data from VesselFinder and enriches with MyShipTracking.
    
    Returns a dict with:
    - Static vessel info (name, type, dimensions, etc.)
    - Current position (lat, lon, sog, cog)
    - Position source indicator
    """
    url = f"{VESSELFINDER_BASE_URL}/{imo}"
    
    # Fetch VesselFinder page
    try:
        response = requests.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
    except requests.Timeout:
        logger.error(f"VesselFinder timeout for IMO {imo}")
        return {"found": False, "imo": imo, "error": "Request timeout"}
    except requests.RequestException as e:
        logger.error(f"VesselFinder request failed for IMO {imo}: {e}")
        return {"found": False, "imo": imo, "error": str(e)}

    if response.status_code == 404:
        logger.info(f"VesselFinder: IMO {imo} not found")
        return {"found": False, "imo": imo, "error": "Vessel not found"}
    
    if response.status_code != 200:
        logger.error(f"VesselFinder returned status {response.status_code} for IMO {imo}")
        return {"found": False, "imo": imo, "error": f"HTTP {response.status_code}"}

    soup = BeautifulSoup(response.text, "html.parser")

    # --- Extract Basic Info ---
    name_el = soup.select_one("h1.title")
    name = name_el.get_text(strip=True) if name_el else f"IMO {imo}"
    
    dest_el = soup.select_one("div.vi__r1.vi__sbt a._npNa")
    destination = dest_el.get_text(strip=True) if dest_el else ""

    info_icon = soup.select_one("svg.ttt1.info")
    last_pos_utc = info_icon.get("data-title") if info_icon and info_icon.has_attr("data-title") else None

    # --- Extract Static Data Tables ---
    tech_data = extract_table_data(soup, "tpt1")
    dims_data = extract_table_data(soup, "tptfix")
    ais_table_data = extract_table_data(soup, "vessel-info-table")
    aparams_data = extract_table_data(soup, "aparams")
    
    static_data = {**tech_data, **dims_data, **ais_table_data, **aparams_data}
    mmsi = extract_mmsi(soup, static_data)

    # --- Extract Draught ---
    draught_val = static_data.get("Current draught") or static_data.get("Draught")
    if not draught_val:
        match = re.search(
            r"(?:draught|draft)\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*m",
            soup.get_text(),
            re.IGNORECASE
        )
        if match:
            draught_val = f"{match.group(1)} m"

    # --- Build Static Data Response ---
    flag_el = soup.select_one("div.title-flag-icon")
    flag = flag_el.get("title") if flag_el else None

    # --- Extract AIS Position from VesselFinder ---
    vf_lat = vf_lon = sog = cog = None
    djson_div = soup.find("div", id="djson")
    
    if djson_div and djson_div.has_attr("data-json"):
        try:
            ais = json.loads(djson_div["data-json"])
            vf_lat = float(ais["ship_lat"]) if ais.get("ship_lat") else None
            vf_lon = float(ais["ship_lon"]) if ais.get("ship_lon") else None
            sog = ais.get("ship_sog")
            cog = ais.get("ship_cog")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            logger.warning(f"VesselFinder: failed to parse djson for IMO {imo}: {e}")

    # --- Try MyShipTracking for Better Position ---
    mst_data = None
    if mmsi and vf_lat is not None and vf_lon is not None:
        mst_data = get_myshiptracking_pos(mmsi, vf_lat, vf_lon)
        if mst_data:
            logger.info(f"MyShipTracking found position for MMSI {mmsi}")

    # --- Select Best Position Source ---
    vf_age = get_vf_age_minutes(last_pos_utc)
    selected_source = "none"
    lat = lon = None

    # Prefer MyShipTracking if VF data is stale or MST has better precision
    mst_valid = mst_data and is_valid_coordinates(mst_data["lat"], mst_data["lon"])
    vf_valid = is_valid_coordinates(vf_lat, vf_lon)

    if mst_valid and vf_valid:
        vf_precision = count_decimals(vf_lat) + count_decimals(vf_lon)
        mst_precision = count_decimals(mst_data["lat"]) + count_decimals(mst_data["lon"])
        
        if vf_age > 60 or mst_precision > vf_precision:
            selected_source = "myshiptracking"
            lat, lon = mst_data["lat"], mst_data["lon"]
            sog = mst_data.get("sog", sog)
            cog = mst_data.get("cog", cog)
        else:
            selected_source = "vesselfinder"
            lat, lon = vf_lat, vf_lon
    elif mst_valid:
        selected_source = "myshiptracking"
        lat, lon = mst_data["lat"], mst_data["lon"]
        sog = mst_data.get("sog")
        cog = mst_data.get("cog")
    elif vf_valid:
        selected_source = "vesselfinder"
        lat, lon = vf_lat, vf_lon

    if lat and lon:
        logger.info(f"IMO {imo}: using {selected_source} position ({lat}, {lon})")
    else:
        logger.warning(f"IMO {imo}: no valid position found")

    return {
        "found": True,
        "imo": imo,
        "vessel_name": name,
        "mmsi": mmsi,
        "ship_type": static_data.get("Ship type") or static_data.get("Type") or static_data.get("Vessel type") or "",
        "flag": flag,
        "lat": lat,
        "lon": lon,
        "sog": sog,
        "cog": cog,
        "destination": destination,
        "last_pos_utc": last_pos_utc,
        "ais_source": selected_source,
        "draught_m": draught_val or "",
        "deadweight_t": static_data.get("Deadweight") or static_data.get("DWT"),
        "gross_tonnage": static_data.get("Gross Tonnage"),
        "year_of_build": static_data.get("Year of Build"),
        "length_overall_m": static_data.get("Length Overall"),
        "beam_m": static_data.get("Beam"),
    }


# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/ping", response_model=HealthResponse, tags=["Health"])
def ping():
    """Health check endpoint."""
    return HealthResponse()


@app.get(
    "/vessel-full/{imo}",
    response_model=VesselResponse,
    tags=["Vessels"],
    responses={
        404: {"description": "Vessel not found"},
        400: {"description": "Invalid IMO number"},
    }
)
def vessel_full(
    imo: str = Path(
        ...,
        title="IMO Number",
        description="7-digit IMO number with valid checksum",
        example="9706900",
        pattern=r"^\d{7}$"
    )
):
    """
    Get full vessel information including current AIS position.
    
    - **imo**: 7-digit IMO number (International Maritime Organization number)
    
    Returns vessel details from VesselFinder enriched with position data
    from MyShipTracking when available.
    """
    # Validate IMO format and checksum
    if not validate_imo(imo):
        raise HTTPException(
            status_code=400,
            detail="Invalid IMO number. Must be 7 digits with valid checksum."
        )
    
    data = scrape_vesselfinder(imo)
    
    if not data.get("found"):
        raise HTTPException(
            status_code=404,
            detail=data.get("error", "Vessel not found")
        )
    
    return VesselResponse(**data)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

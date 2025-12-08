import json
import re
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, Optional

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
# HTML HELPERS – VESSELFINDER
# ============================================================

def extract_table_data(soup: BeautifulSoup, table_class: str) -> Dict[str, str]:
    """
    Extracts key-value pairs from specific tables based on CSS classes.
    Looks for rows with label in tpc1/tpx1 and value in tpc2/tpx2.
    """
    data: Dict[str, str] = {}
    tables = soup.find_all(class_=table_class)
    if not tables:
        return data

    for table in tables:
        for row in table.find_all("tr"):
            label_el = row.find(class_=lambda x: x and ("tpc1" in x or "tpx1" in x))
            value_el = row.find(class_=lambda x: x and ("tpc2" in x or "tpx2" in x))
            if not (label_el and value_el):
                continue

            # Remove small-tag content like "(m)", "(t)" etc.
            label_parts = [c.strip() for c in label_el.contents if isinstance(c, str)]
            label = " ".join(label_parts).replace(":", "").strip()
            value = value_el.get_text(strip=True)

            if label:
                data[label] = value

    return data


def extract_mmsi(soup: BeautifulSoup, static_data: Dict[str, str]) -> Optional[str]:
    """
    Extract MMSI from inline JS (preferred) or from tables as fallback.
    JS example: var MMSI=538005492;
    """
    # 1) Try inline JS
    for s in soup.find_all("script"):
        if not s.string:
            continue
        m = re.search(r"MMSI\s*=\s*(\d+)", s.string)
        if m:
            return m.group(1)

    # 2) Try exact key
    if "MMSI" in static_data:
        v = static_data["MMSI"].strip()
        if v:
            return v

    # 3) Try any label containing MMSI (e.g. "MMSI / Call Sign")
    for key, value in static_data.items():
        if "MMSI" in key.upper():
            v = value.strip()
            if v:
                return v

    return None


# ============================================================
# SHIPFINDER HELPERS (USING MMSI + VF POSITION)
# ============================================================

def make_bounds(lat: float, lon: float, pad: float = 0.25) -> str:
    """
    Build ShipFinder bounds string around a center (lat, lon).
    bounds = south,west,north,east
    """
    south = lat - pad
    west = lon - pad
    north = lat + pad
    east = lon + pad
    return f"{south},{west},{north},{east}"


def get_shipfinder_pos(mmsi: str, center_lat: Optional[float], center_lon: Optional[float]) -> Optional[Dict[str, Any]]:
    """
    Query ShipFinder shipDeltaUpdate endpoint around VF position to get live AIS.
    Returns dict with lat/lon/sog/cog/... or None if not found.
    """
    if center_lat is None or center_lon is None:
        return None

    bounds = make_bounds(center_lat, center_lon)

    url = "https://shipfinder.co/endpoints/shipDeltaUpdate.php"
    params = {"bounds": bounds}

    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=10)
    except requests.RequestException:
        return None

    if r.status_code != 200:
        return None

    try:
        data = r.json()
    except ValueError:
        return None

    ships = data.get("ships", {})
    rec = ships.get(str(mmsi))
    if not rec or len(rec) < 7:
        return None

    try:
        lat_str, lon_str, sog_str, cog_str, heading_str, status_str, ts_str = rec
        return {
            "lat": float(lat_str),
            "lon": float(lon_str),
            "sog": float(sog_str),
            "cog": float(cog_str),
            "heading": float(heading_str),
            "nav_status": int(status_str),
            "timestamp": int(ts_str),
            "representativeTimestamp": data.get("representativeTimestamp"),
        }
    except (ValueError, TypeError):
        return None


# ============================================================
# MAIN SCRAPER – VESSELFINDER + SHIPFINDER FALLBACK
# ============================================================

def scrape_vf_full(imo: str) -> Dict[str, Any]:
    url = f"https://www.vesselfinder.com/vessels/details/{imo}"
    r = requests.get(url, headers=HEADERS, timeout=20)

    if r.status_code == 404:
        return {"found": False, "imo": imo}
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # 1. CORE INFO
    name_el = soup.select_one("h1.title")
    name = name_el.get_text(strip=True) if name_el else f"IMO {imo}"

    dest_el = soup.select_one("div.vi__r1.vi__sbt a._npNa")
    destination = dest_el.get_text(strip=True) if dest_el else ""

    info_icon = soup.select_one("svg.ttt1.info")
    last_pos_utc = (
        info_icon["data-title"]
        if info_icon is not None and info_icon.has_attr("data-title")
        else None
    )

    flag_el = soup.select_one("div.title-flag-icon")
    flag = flag_el.get("title") if flag_el else None

    vessel_type_el = soup.select_one("h2.vst")
    vessel_type = (
        vessel_type_el.get_text(strip=True).split(",")[0].strip()
        if vessel_type_el
        else None
    )

    # 2. STATIC TABLES
    tech_data = extract_table_data(soup, "tpt1")
    dims_data = extract_table_data(soup, "tptfix")
    static_data = {**tech_data, **dims_data}

    mmsi = extract_mmsi(soup, static_data)

    final_static_data: Dict[str, Any] = {
        "imo": imo,
        "vessel_name": name,
        "ship_type": vessel_type,
        "flag": flag,
        "mmsi": mmsi,
        "deadweight_t": static_data.get("Deadweight") or static_data.get("DWT"),
        "gross_tonnage": static_data.get("Gross Tonnage"),
        "year_of_build": static_data.get("Year of Build"),
        "length_overall_m": static_data.get("Length Overall"),
        "beam_m": static_data.get("Beam"),
    }

    # 3. AIS FROM VESSELFINDER
    vf_lat = vf_lon = vf_sog = vf_cog = None
    djson_div = soup.find("div", id="djson")

    if djson_div and djson_div.has_attr("data-json"):
        try:
            ais = json.loads(djson_div["data-json"])
            vf_lat = ais.get("ship_lat")
            vf_lon = ais.get("ship_lon")
            vf_sog = ais.get("ship_sog")
            vf_cog = ais.get("ship_cog")
        except (json.JSONDecodeError, TypeError):
            pass

    # 4. OPTIONAL OVERRIDE FROM SHIPFINDER (LIVE AIS, USING VF POSITION AS CENTER)
    sf_data: Optional[Dict[str, Any]] = None
    if mmsi:
        sf_data = get_shipfinder_pos(mmsi, vf_lat, vf_lon)

    # Decide final AIS values
    lat = sf_data["lat"] if sf_data and sf_data.get("lat") is not None else vf_lat
    lon = sf_data["lon"] if sf_data and sf_data.get("lon") is not None else vf_lon
    sog = sf_data["sog"] if sf_data and sf_data.get("sog") is not None else vf_sog
    cog = sf_data["cog"] if sf_data and sf_data.get("cog") is not None else vf_cog

    result: Dict[str, Any] = {
        "found": True,
        "destination": destination,
        "last_pos_utc": last_pos_utc,
        **final_static_data,
        "lat": lat,
        "lon": lon,
        "sog": sog,
        "cog": cog,
    }

    # Optionally expose ShipFinder-specific fields
    if sf_data:
        result["sf_heading"] = sf_data.get("heading")
        result["sf_nav_status"] = sf_data.get("nav_status")
        result["sf_timestamp"] = sf_data.get("timestamp")
        result["sf_representative_ts"] = sf_data.get("representativeTimestamp")

    return result


# ============================================================
# API ENDPOINT
# ============================================================

@app.get("/vessel-full/{imo}")
def vessel_full(imo: str):
    data = scrape_vf_full(imo)
    if not data.get("found"):
        raise HTTPException(status_code=404, detail="Vessel not found")
    return data

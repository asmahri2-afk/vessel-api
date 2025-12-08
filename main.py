import json
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, Optional

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.vesselfinder.com/",
}

app = FastAPI()

# ─────────────────────────────────────
# WARM-UP ENDPOINT
# ─────────────────────────────────────
@app.get("/ping")
def ping():
    return {"ok": True}

# ─────────────────────────────────────
# CORS
# ─────────────────────────────────────
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


# ─────────────────────────────────────
# TABLE EXTRACTION FUNCTION
# ─────────────────────────────────────
def extract_table_data(soup: BeautifulSoup, table_class: str) -> Dict[str, str]:
    """Extract key–value pairs from Vesselfinder info tables."""
    data: Dict[str, str] = {}

    tables = soup.find_all(class_=table_class)
    if not tables:
        return data

    for table in tables:
        for row in table.find_all("tr"):
            label_el = row.find(class_=lambda x: x and ("tpc1" in x or "tpx1" in x))
            value_el = row.find(class_=lambda x: x and ("tpc2" in x or "tpx2" in x))

            if label_el and value_el:
                label_parts = [c.strip() for c in label_el.contents if isinstance(c, str)]
                label = " ".join(label_parts).replace(":", "").strip()
                value = value_el.get_text(strip=True)

                if label:
                    data[label] = value

    return data


# ─────────────────────────────────────
# MMSI EXTRACTION (ROBUST)
# ─────────────────────────────────────
def extract_mmsi(static_data: Dict[str, str]) -> Optional[str]:
    """
    MMSI label on VF can be:
      - "MMSI"
      - "MMSI / Call Sign"
      - "MMSI/Callsign"
    This function returns MMSI regardless of naming.
    """
    # direct key
    if "MMSI" in static_data:
        v = static_data["MMSI"].strip()
        return v or None

    # search in keys
    for key, value in static_data.items():
        if "MMSI" in key.upper():
            v = value.strip()
            return v or None

    return None


# ─────────────────────────────────────
# SCRAPER FUNCTION
# ─────────────────────────────────────
def scrape_vf_full(imo: str) -> Dict[str, Any]:
    url = f"https://www.vesselfinder.com/vessels/details/{imo}"
    r = requests.get(url, headers=HEADERS, timeout=20)

    if r.status_code == 404:
        return {"found": False, "imo": imo}
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # 1 — CORE FIELDS
    name_el = soup.select_one("h1.title")
    name = name_el.get_text(strip=True) if name_el else f"IMO {imo}"

    dest_el = soup.select_one("div.vi__r1.vi__sbt a._npNa")
    destination = dest_el.get_text(strip=True) if dest_el else ""

    info_icon = soup.select_one("svg.ttt1.info")
    last_pos_utc = (
        info_icon.get("data-title")
        if info_icon and info_icon.has_attr("data-title")
        else None
    )

    flag_el = soup.select_one("div.title-flag-icon")
    flag = flag_el.get("title") if flag_el else None

    vessel_type_el = soup.select_one("h2.vst")
    vessel_type = (
        vessel_type_el.get_text(strip=True).split(",")[0].strip()
        if vessel_type_el else None
    )

    # 2 — STATIC DATA TABLES
    tech_data = extract_table_data(soup, "tpt1")
    dims_data = extract_table_data(soup, "tptfix")
    static_data = {**tech_data, **dims_data}

    mmsi = extract_mmsi(static_data)

    static_parsed = {
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

    # 3 — AIS DATA FROM VF JSON
    djson_div = soup.find("div", id="djson")
    vf_lat = vf_lon = vf_sog = vf_cog = None

    if djson_div and djson_div.has_attr("data-json"):
        try:
            ais = json.loads(djson_div["data-json"])
            vf_lat = ais.get("ship_lat")
            vf_lon = ais.get("ship_lon")
            vf_sog = ais.get("ship_sog")
            vf_cog = ais.get("ship_cog")
        except:
            pass

    data = {
        "found": True,
        "destination": destination,
        "last_pos_utc": last_pos_utc,
        **static_parsed,
        "lat": vf_lat,
        "lon": vf_lon,
        "sog": vf_sog,
        "cog": vf_cog,
    }

    return data


# ─────────────────────────────────────
# API ENDPOINT
# ─────────────────────────────────────
@app.get("/vessel-full/{imo}")
def vessel_full(imo: str):
    data = scrape_vf_full(imo)
    if not data.get("found"):
        raise HTTPException(status_code=404, detail="Vessel not found")
    return data

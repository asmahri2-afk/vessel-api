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
                class_=lambda x: x and ("tpc1" in x or "tpx1" in x)
            )
            value_el = row.find(
                class_=lambda x: x and ("tpc2" in x or "tpx2" in x)
            )
            if not (label_el and value_el):
                continue

            label_parts = [
                c.strip() for c in label_el.contents if isinstance(c, str)
            ]
            label = " ".join(label_parts).replace(":", "").strip()
            value = value_el.get_text(strip=True)

            if label:
                data[label] = value

    return data


def extract_mmsi(soup: BeautifulSoup, static_data: Dict[str, str]) -> Optional[str]:
    # Inline JS: var MMSI=538005492;
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
    pad: float = 0.7,
) -> Optional[Dict[str, Any]]:
    """
    Utilise l'endpoint MyShipTracking 'vesselsonmaptempTTT.php'
    dans une petite bbox autour de la position VF, et retourne
    les infos AIS pour le MMSI donné si trouvé.
    """
    if center_lat is None or center_lon is None:
        return None

    try:
        lat_f = float(center_lat)
        lon_f = float(center_lon)
    except (TypeError, ValueError):
        return None

    minlat = lat_f - pad
    maxlat = lat_f + pad
    minlon = lon_f - pad
    maxlon = lon_f + pad

    filters_str = json.dumps({
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
    })

    params = {
        "type": "json",
        "minlat": minlat,
        "maxlat": maxlat,
        "minlon": minlon,
        "maxlon": maxlon,
        "zoom": 12,      # comme dans ton payload console
        "selid": -1,
        "seltype": 0,
        "timecode": -1,
        "filters": filters_str,
    }

    mst_headers = dict(HEADERS)
    mst_headers["Referer"] = "https://www.myshiptracking.com/"

    try:
        r = requests.get(
            MYSHIPTRACKING_URL,
            params=params,
            headers=mst_headers,
            timeout=10,
        )
    except requests.RequestException:
        return None

    if r.status_code != 200:
        return None

    try:
        data = r.json()
    except ValueError:
        return None

    if not isinstance(data, list):
        return None

    candidate = None
    for ship in data:
        ship_mmsi = str(
            ship.get("mmsi")
            or ship.get("MMSI")
            or ""
        ).strip()
        if ship_mmsi == str(mmsi):
            candidate = ship
            break

    if not candidate:
        return None

    try:
        lat = float(candidate.get("lat") or candidate.get("LAT"))
        lon = float(candidate.get("lon") or candidate.get("LON"))
    except (TypeError, ValueError):
        return None

    sog = candidate.get("speed") or candidate.get("SPEED") or candidate.get("sog")
    cog = candidate.get("course") or candidate.get("COURSE") or candidate.get("cog")

    try:
        sog_f = float(sog) if sog is not None else None
    except (TypeError, ValueError):
        sog_f = None

    try:
        cog_f = float(cog) if cog is not None else None
    except (TypeError, ValueError):
        cog_f = None

    return {
        "lat": lat,
        "lon": lon,
        "sog": sog_f,
        "cog": cog_f,
        "timestamp": candidate.get("lastpos") or candidate.get("LASTPOS"),
        "mst_raw": candidate,
    }

# ============================================================
# MAIN SCRAPER – VESSELFINDER + MYSHIPTRACKING OVERRIDE
# ============================================================

def scrape_vf_full(imo: str) -> Dict[str, Any]:
    url = f"https://www.vesselfinder.com/vessels/details/{imo}"
    r = requests.get(url, headers=HEADERS, timeout=20)

    if r.status_code == 404:
        return {"found": False, "imo": imo}
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

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
        if vessel_type_el else None
    )

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

    # AIS VESSELFINDER
    vf_lat = vf_lon = None
    sog = cog = None
    djson_div = soup.find("div", id="djson")

    if djson_div and djson_div.has_attr("data-json"):
        try:
            ais = json.loads(djson_div["data-json"])
            ship_lat = ais.get("ship_lat")
            ship_lon = ais.get("ship_lon")
            sog = ais.get("ship_sog")
            cog = ais.get("ship_cog")
            try:
                vf_lat = float(ship_lat) if ship_lat is not None else None
                vf_lon = float(ship_lon) if ship_lon is not None else None
            except (TypeError, ValueError):
                vf_lat = vf_lon = None
        except Exception:
            pass

    # OVERRIDE AVEC MYSHIPTRACKING SI POSSIBLE
    mst_data: Optional[Dict[str, Any]] = None
    if mmsi and vf_lat is not None and vf_lon is not None:
        mst_data = get_myshiptracking_pos(mmsi, vf_lat, vf_lon)

    if mst_data:
        lat = mst_data["lat"]
        lon = mst_data["lon"]
        if mst_data.get("sog") is not None:
            sog = mst_data["sog"]
        if mst_data.get("cog") is not None:
            cog = mst_data["cog"]
        ais_source = "myshiptracking"
    else:
        lat = vf_lat
        lon = vf_lon
        ais_source = "vesselfinder"

    result: Dict[str, Any] = {
        "found": True,
        "destination": destination,
        "last_pos_utc": last_pos_utc,
        **final_static_data,
        "lat": lat,
        "lon": lon,
        "sog": sog,
        "cog": cog,
        "ais_source": ais_source,
    }

    if mst_data:
        result["mst_timestamp"] = mst_data.get("timestamp")

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

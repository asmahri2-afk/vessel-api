# (Full file unchanged – provided for completeness)
import json
import time
import logging
import os
import re
import requests
import httpx
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import copy
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from openpyxl import load_workbook
from openpyxl.styles import Font, Border, Side, PatternFill, Alignment

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

import random

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
]

def _make_headers(referer: str = "https://www.vesselfinder.com/") -> dict:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": referer,
        "DNT": "1",
    }

HEADERS = _make_headers()

MYSHIPTRACKING_URL = "https://www.myshiptracking.com/requests/vesselsonmaptempTTT.php"

API_SECRET         = os.getenv("API_SECRET", "")
EQUASIS_EMAIL      = os.getenv("EQUASIS_EMAIL", "")
EQUASIS_PASSWORD   = os.getenv("EQUASIS_PASSWORD", "")

BATCH_MAX_WORKERS = 2
BATCH_MAX_IMOS    = 50

# ============================================================
# FASTAPI APP + CORS
# ============================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# REQUEST MODELS
# ============================================================

class BatchRequest(BaseModel):
    imos: List[str]

# ============================================================
# UTILITY HELPERS
# ============================================================

def count_decimals(val: Any) -> int:
    if val is None:
        return 0
    s = str(val)
    if "." in s:
        return len(s.split(".")[-1].rstrip("0"))
    return 0

def parse_vf_timestamp(ts: Optional[str]) -> Optional[datetime]:
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
    dt = parse_vf_timestamp(last_pos_utc)
    if not dt:
        return 999
    age = (datetime.now(timezone.utc) - dt).total_seconds() / 60
    return int(age)

# ============================================================
# INPUT VALIDATION
# ============================================================

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

    mst_headers = _make_headers(referer="https://www.myshiptracking.com/")

    try:
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

    except Exception as e:
        logger.warning(f"MyShipTracking fetch failed for MMSI {mmsi}: {e}")

    return None

# ============================================================
# MAIN SCRAPER
# ============================================================

def scrape_vf_full(imo: str, session: requests.Session) -> Dict[str, Any]:
    url = f"https://www.vesselfinder.com/vessels/details/{imo}"

    r = session.get(url, headers=_make_headers(), timeout=20)

    if r.status_code == 404:
        logger.info(f"IMO {imo} returned 404 from VesselFinder")
        return {"found": False, "imo": imo}
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    name_el = soup.select_one("h1.title")
    name = name_el.get_text(strip=True) if name_el else f"IMO {imo}"
    dest_el = soup.select_one("div.vi__r1.vi__sbt a._npNa")
    destination = dest_el.get_text(strip=True) if dest_el else ""

    info_icon = soup.select_one("svg.ttt1.info")
    last_pos_utc = info_icon["data-title"] if info_icon and info_icon.has_attr("data-title") else None
    logger.info(f"IMO {imo} | name={name} | last_pos_utc={last_pos_utc}")

    tech_data      = extract_table_data(soup, "tpt1")
    dims_data      = extract_table_data(soup, "tptfix")
    ais_table_data = extract_table_data(soup, "vessel-info-table")
    aparams_data   = extract_table_data(soup, "aparams")
    static_data    = {**tech_data, **dims_data, **ais_table_data, **aparams_data}
    mmsi           = extract_mmsi(soup, static_data)

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

    vf_lat = vf_lon = sog = cog = None
    djson_div = soup.find("div", id="djson")
    if djson_div and djson_div.has_attr("data-json"):
        try:
            ais = json.loads(djson_div["data-json"])
            vf_lat = float(ais.get("ship_lat")) if ais.get("ship_lat") else None
            vf_lon = float(ais.get("ship_lon")) if ais.get("ship_lon") else None
            sog = ais.get("ship_sog")
            cog = ais.get("ship_cog")
            logger.info(f"IMO {imo} | VF AIS: lat={vf_lat}, lon={vf_lon}, sog={sog}, cog={cog}")
        except Exception as e:
            logger.warning(f"IMO {imo} | Failed to parse djson AIS data: {e}")

    mst_data = get_myshiptracking_pos(mmsi, vf_lat, vf_lon, session) if (mmsi and vf_lat) else None

    use_mst = False
    vf_age  = get_vf_age_minutes(last_pos_utc)
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
# API ENDPOINTS
# ============================================================

def _check_auth(request: Request, imo: str = ""):
    if API_SECRET:
        client_secret = request.headers.get("X-API-Secret", "")
        if client_secret != API_SECRET:
            logger.warning(f"Unauthorized request for IMO {imo} from {request.client.host}")
            raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/ping")
def ping():
    return {"ok": True}

@app.get("/vessel-full/{imo}")
def vessel_full(imo: str, request: Request):
    _check_auth(request, imo)

    if not validate_imo(imo):
        logger.warning(f"Invalid IMO rejected: {imo}")
        raise HTTPException(status_code=400, detail="Invalid IMO number")

    with requests.Session() as session:
        try:
            data = scrape_vf_full(imo, session)
        except Exception as e:
            logger.error(f"Scrape failed for IMO {imo}: {e}", exc_info=True)
            raise HTTPException(status_code=502, detail="Upstream scrape failed")

    if not data.get("found"):
        raise HTTPException(status_code=404, detail="Vessel not found")

    return data

@app.post("/vessel-batch")
def vessel_batch(body: BatchRequest, request: Request):
    _check_auth(request)

    imos = list(dict.fromkeys(body.imos))

    if len(imos) > BATCH_MAX_IMOS:
        raise HTTPException(status_code=400, detail=f"Too many IMOs — max {BATCH_MAX_IMOS} per batch")

    invalid = [imo for imo in imos if not validate_imo(imo)]
    if invalid:
        raise HTTPException(status_code=400, detail=f"Invalid IMOs: {', '.join(invalid)}")

    results: Dict[str, Any] = {}
    errors:  Dict[str, str] = {}

    def fetch_one(imo: str) -> tuple:
        try:
            time.sleep(random.uniform(2, 5))
            with requests.Session() as session:
                data = scrape_vf_full(imo, session)
            if not data.get("found"):
                return imo, None, "Vessel not found"
            return imo, data, None
        except Exception as e:
            logger.error(f"Batch scrape failed for IMO {imo}: {e}")
            return imo, None, str(e)

    logger.info(f"Batch request: {len(imos)} vessels, {BATCH_MAX_WORKERS} workers")

    with ThreadPoolExecutor(max_workers=BATCH_MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_one, imo): imo for imo in imos}
        for future in as_completed(futures):
            imo, data, error = future.result()
            if error:
                errors[imo] = error
            else:
                results[imo] = data

    logger.info(f"Batch complete: {len(results)} success, {len(errors)} errors")

    return {
        "results": results,
        "errors":  errors,
        "total":   len(imos),
        "success": len(results),
        "failed":  len(errors),
    }

# ============================================================
# EQUASIS SCRAPER
# ============================================================

EQUASIS_LOGIN_URL  = "https://www.equasis.org/EquasisWeb/authen/HomePage"
EQUASIS_VESSEL_URL = "https://www.equasis.org/EquasisWeb/restricted/ShipInfo"

def _equasis_session() -> requests.Session:
    if not EQUASIS_EMAIL or not EQUASIS_PASSWORD:
        raise HTTPException(status_code=503, detail="Equasis credentials not configured")

    session = requests.Session()
    headers = {
        "User-Agent": random.choice(_USER_AGENTS),
        "Referer": EQUASIS_LOGIN_URL,
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    r = session.post(
        EQUASIS_LOGIN_URL,
        data={"j_email": EQUASIS_EMAIL, "j_password": EQUASIS_PASSWORD, "submit": "Login"},
        headers=headers,
        timeout=15,
        allow_redirects=True,
    )
    r.raise_for_status()

    if "j_password" in r.text or "invalid" in r.text.lower() and "password" in r.text.lower():
        raise HTTPException(status_code=502, detail="Equasis login failed — check credentials")

    return session

def scrape_equasis(imo: str) -> Dict[str, Any]:
    session = _equasis_session()

    headers = {
        "User-Agent": random.choice(_USER_AGENTS),
        "Referer": EQUASIS_LOGIN_URL,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    r = session.get(
        EQUASIS_VESSEL_URL,
        params={"fs": "Search", "P_IMO": imo},
        headers=headers,
        timeout=15,
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    if "no vessel found" in text.lower() or "not found" in text.lower():
        return {"found": False, "imo": imo}

    result: Dict[str, Any] = {"imo": imo, "found": True}

    def parse_tables(soup_obj):
        pairs = {}
        for table in soup_obj.find_all("table"):
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                for i in range(len(cells) - 1):
                    label = cells[i].get_text(" ", strip=True).rstrip(":").strip()
                    value = cells[i + 1].get_text(" ", strip=True).strip()
                    if label and value and len(label) < 60:
                        pairs[label] = value
        return pairs

    pairs = parse_tables(soup)

    for sel in ["h1", "h2", ".ship-name", ".title", "#shipName"]:
        el = soup.select_one(sel)
        if el:
            name = el.get_text(strip=True)
            if name and len(name) > 2:
                result["vessel_name"] = name
                break

    FIELD_MAP = {
        "vessel_name":   ["Ship name", "Name", "Vessel name"],
        "Flag":          ["Flag", "Flag State", "Flag state"],
        "Type of ship":  ["Type of ship", "Ship type", "Type"],
        "MMSI":          ["MMSI"],
        "Call Sign":     ["Call sign", "Call Sign", "Callsign"],
        "Gross tonnage": ["Gross tonnage", "GT", "Gross Tonnage"],
        "DWT":           ["Deadweight", "DWT", "Deadweight (t)"],
        "Year of build": ["Year of build", "Built", "Year built", "Year of Build"],
        "Status":        ["Status", "Ship status"],
        "IMO":           ["IMO number", "IMO No", "IMO"],
    }
    for out_key, candidates in FIELD_MAP.items():
        for candidate in candidates:
            if candidate in pairs and pairs[candidate]:
                result[out_key] = pairs[candidate]
                break

    OWNER_KEYWORDS   = ["registered owner", "owner"]
    PI_KEYWORDS      = ["p&i club", "p & i", "protection", "p and i"]
    CLASS_KEYWORDS   = ["class", "classification"]

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            role = cells[0].get_text(" ", strip=True).lower()
            company_name = cells[1].get_text(" ", strip=True)

            address = cells[2].get_text(" ", strip=True) if len(cells) > 2 else ""

            if any(k in role for k in OWNER_KEYWORDS) and company_name:
                if "equasis_owner" not in result:
                    result["equasis_owner"] = company_name
                    if address:
                        result["equasis_address"] = address

            elif any(k in role for k in PI_KEYWORDS) and company_name:
                if "pi_club" not in result:
                    result["pi_club"] = company_name

            elif any(k in role for k in CLASS_KEYWORDS) and company_name:
                if "class_society" not in result:
                    result["class_society"] = company_name

    if "equasis_owner" not in result:
        m = re.search(r"Registered\s+owner\s*[:\-]?\s*([A-Z][^\n\r]{3,80})", text, re.IGNORECASE)
        if m:
            result["equasis_owner"] = m.group(1).strip()

    logger.info(
        f"IMO {imo} | Equasis: name={result.get('vessel_name')} "
        f"owner={result.get('equasis_owner')} flag={result.get('Flag')}"
    )
    return result

@app.get("/equasis/{imo}")
def equasis_vessel(imo: str, request: Request):
    _check_auth(request, imo)

    if not validate_imo(imo):
        logger.warning(f"Equasis: invalid IMO rejected: {imo}")
        raise HTTPException(status_code=400, detail="Invalid IMO number")

    try:
        data = scrape_equasis(imo)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Equasis scrape failed for IMO {imo}: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Equasis scrape failed: {str(e)}")

    if not data.get("found"):
        raise HTTPException(status_code=404, detail="Vessel not found on Equasis")

    return data

# ============================================================
# SOF — STATEMENT OF FACTS GENERATOR
# ============================================================

DAYS = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

class SOFRow(BaseModel):
    date:    Optional[str] = ''
    wfrom:   Optional[str] = ''
    wto:     Optional[str] = ''
    sfrom:   Optional[str] = ''
    sto:     Optional[str] = ''
    cranes:  Optional[str] = ''
    qty:     Optional[str] = ''
    remarks: Optional[str] = ''

class SOFData(BaseModel):
    agent:             Optional[str] = ''
    operation_type:    Optional[str] = 'import'
    vessel:            Optional[str] = ''
    port:              Optional[str] = ''
    owners:            Optional[str] = ''
    cargo:             Optional[str] = ''
    bl_weight:         Optional[str] = ''
    bl_number:         Optional[str] = ''
    nor_accepted:      Optional[str] = 'AS PER TERMS AND CONDITIONS OF THE RELEVENT C/P.'
    port_hours:        Optional[str] = ''
    general_remarks:   Optional[str] = ''
    remarks:           Optional[str] = ''
    master_remarks:    Optional[str] = ''
    berthed_date:      Optional[str] = ''
    berthed_time:      Optional[str] = ''
    disch_start_date:  Optional[str] = ''
    disch_start_time:  Optional[str] = ''
    disch_end_date:    Optional[str] = ''
    disch_end_time:    Optional[str] = ''
    cargo_docs_date:   Optional[str] = ''
    cargo_docs_time:   Optional[str] = ''
    sailing_date:      Optional[str] = ''
    sailing_time:      Optional[str] = ''
    eosp_date:         Optional[str] = ''
    eosp_time:         Optional[str] = ''
    nor_tender_date:   Optional[str] = ''
    nor_tender_time:   Optional[str] = ''
    anchor_drop_date:  Optional[str] = ''
    anchor_drop_time:  Optional[str] = ''
    anchor_weigh_date: Optional[str] = ''
    anchor_weigh_time: Optional[str] = ''
    pilot_date:        Optional[str] = ''
    pilot_time:        Optional[str] = ''
    rows:              List[SOFRow] = []

def fmt_dt(date: str, time: str) -> str:
    if not date:
        return ''
    try:
        parts = date.split('-')
        d = f"{parts[2]}/{parts[1]}/{parts[0]}" if len(parts) == 3 else date
    except Exception:
        d = date
    t = time.replace(':', '') if time else ''
    return f"{d} at   {t} hr" if t else d

def get_day_name(date_str: str) -> str:
    if not date_str:
        return ''
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return DAYS[dt.weekday()]
    except Exception:
        return ''

SOF_TEMPLATE_BYTES: Optional[bytes] = None

async def get_sof_template() -> bytes:
    global SOF_TEMPLATE_BYTES
    if SOF_TEMPLATE_BYTES:
        return SOF_TEMPLATE_BYTES
    url = 'https://asmahri2-afk.github.io/test/SOF_TEMPLATE.xlsx'
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url)
        r.raise_for_status()
        SOF_TEMPLATE_BYTES = r.content
        logger.info(f"SOF template loaded: {len(SOF_TEMPLATE_BYTES)} bytes")
        return SOF_TEMPLATE_BYTES

@app.post('/sof/generate')
async def sof_generate(data: SOFData, request: Request):
    if API_SECRET:
        client_secret = request.headers.get('X-API-Secret', '')
        if client_secret != API_SECRET:
            raise HTTPException(status_code=401, detail='Unauthorized')

    try:
        template_bytes = await get_sof_template()
        wb = load_workbook(io.BytesIO(template_bytes))
        ws = wb.active

        operation_verb = 'loading' if (data.operation_type or '').lower() == 'export' else 'discharging'

        tag_values = {
            '{{AGENT}}':          data.agent or '',
            '{{VESSEL_NAME}}':    data.vessel or '',
            '{{PORT}}':           data.port or '',
            '{{OWNERS}}':         data.owners or '',
            '{{CARGO}}':          data.cargo or '',
            '{{BL_WEIGHT}}':      data.bl_weight or '',
            '{{BL_NUMBER}}':      data.bl_number or '',
            '{{PORT_HOURS}}':     data.port_hours or '',
            '{{GENERAL_REMARKS}}': data.general_remarks or '',
            '{{REMARKS}}':        data.remarks or '',
            '{{MASTER_REMARKS}}': data.master_remarks or '',
            '{{NOR_ACCEPTED}}':   data.nor_accepted or '',
            '{{OPERATION_VERB}}': operation_verb,
            '{{BERTHED_DATE}} at {{BERTHED_TIME}} hr':         fmt_dt(data.berthed_date, data.berthed_time),
            '{{DISCH_START_DATE}} at {{DISCH_START_TIME}} hr': fmt_dt(data.disch_start_date, data.disch_start_time),
            '{{DISCH_END_DATE}} at {{DISCH_END_TIME}} hr':     fmt_dt(data.disch_end_date, data.disch_end_time),
            '{{CARGO_DOCS_DATE}} at {{CARGO_DOCS_TIME}} hr':   fmt_dt(data.cargo_docs_date, data.cargo_docs_time),
            '{{SAILING_DATE}} at {{SAILING_TIME}} hr':         fmt_dt(data.sailing_date, data.sailing_time),
            '{{EOSP_DATE}} at {{EOSP_TIME}} hr':               fmt_dt(data.eosp_date, data.eosp_time),
            '{{NOR_TENDER_DATE}} at {{NOR_TENDER_TIME}} hr':   fmt_dt(data.nor_tender_date, data.nor_tender_time),
            '{{ANCHOR_DROP_DATE}} at {{ANCHOR_DROP_TIME}} hr': fmt_dt(data.anchor_drop_date, data.anchor_drop_time),
            '{{ANCHOR_WEIGH_DATE}} at {{ANCHOR_WEIGH_TIME}} hr': fmt_dt(data.anchor_weigh_date, data.anchor_weigh_time),
            '{{PILOT_DATE}} at {{PILOT_TIME}} hr':             fmt_dt(data.pilot_date, data.pilot_time),
            'M/V {{VESSEL_NAME}}': f"M/V {data.vessel or ''}",
        }

        for row in ws.iter_rows():
            for cell in row:
                if cell.value and isinstance(cell.value, str):
                    val = cell.value
                    for tag, replacement in tag_values.items():
                        if tag in val:
                            val = val.replace(tag, replacement)
                    cell.value = val

        marker_row = template_row = end_row = None
        for row in ws.iter_rows():
            for cell in row:
                if cell.value == '{{#EACH_ROW}}':   marker_row = cell.row
                elif cell.value == '{{ROW_DATE}}':   template_row = cell.row
                elif cell.value == '{{/EACH_ROW}}':  end_row = cell.row

        if marker_row and template_row and data.rows:
            tpl_styles = {}
            for col in range(1, 12):
                c = ws.cell(row=template_row, column=col)
                tpl_styles[col] = {
                    'font':      copy(c.font),
                    'border':    copy(c.border),
                    'fill':      copy(c.fill),
                    'alignment': copy(c.alignment),
                }

            for r in filter(None, [marker_row, template_row, end_row]):
                for col in range(1, 12):
                    ws.cell(row=r, column=col).value = None

            for i, row_data in enumerate(data.rows):
                r = marker_row + i
                values = [
                    fmt_dt(row_data.date, '').split(' ')[0] if row_data.date else '',
                    get_day_name(row_data.date),
                    row_data.wfrom.replace(':','') if row_data.wfrom else '',
                    row_data.wto.replace(':','') if row_data.wto else '',
                    row_data.sfrom.replace(':','') if row_data.sfrom else '',
                    row_data.sto.replace(':','') if row_data.sto else '',
                    row_data.cranes or '',
                    row_data.qty or '',
                    '',
                    row_data.remarks or '',
                    '',
                ]
                for col, val in enumerate(values, 1):
                    cell = ws.cell(row=r, column=col)
                    cell.value = val if val else None
                    s = tpl_styles.get(col, {})
                    if s.get('font'):      cell.font = s['font']
                    if s.get('border'):    cell.border = s['border']
                    if s.get('fill'):      cell.fill = s['fill']
                    if s.get('alignment'): cell.alignment = s['alignment']

        if (data.agent or '').upper() == 'COMANAV' and ws._images:
            try:
                comanav_url = 'https://asmahri2-afk.github.io/test/logo-comanav.png'
                async with httpx.AsyncClient(timeout=10) as client:
                    logo_resp = await client.get(comanav_url)
                    logo_resp.raise_for_status()
                    logo_bytes = logo_resp.content

                old_img = ws._images[0]
                old_img.ref = io.BytesIO(logo_bytes)
                logger.info(f"Swapped logo to COMANAV ({len(logo_bytes)} bytes)")
            except Exception as e:
                logger.warning(f"Logo swap failed (non-critical): {type(e).__name__}: {e}", exc_info=True)

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)

        vessel = (data.vessel or 'VESSEL').replace(' ', '_')
        port = (data.port or 'PORT').replace(' ', '_')
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"SOF_{vessel}_{port}_{date_str}.xlsx"

        return Response(
            content=buf.read(),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )

    except Exception as e:
        logger.error(f"SOF generation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"SOF generation failed: {str(e)}")

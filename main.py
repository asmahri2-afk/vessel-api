# ============================================================
# IMPORTS
# ============================================================

import json
import logging
import os
import re
import requests
import httpx
import io
import random
import time

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

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================

WORKER_URL = "https://vesseltracker.asmahri1.workers.dev/fetch?url="
REQUEST_TIMEOUT = 10
MAX_RETRIES = 2

MYSHIPTRACKING_URL = "https://www.myshiptracking.com/requests/vesselsonmaptempTTT.php"

BATCH_SIZE = 3
BATCH_COOLDOWN = 10

# ============================================================
# HEADERS
# ============================================================

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
]

def _make_headers():
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml",
        "Referer": "https://www.vesselfinder.com/",
        "Connection": "keep-alive",
    }

# ============================================================
# SESSION
# ============================================================

def create_session():
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

# ============================================================
# SAFE REQUEST
# ============================================================

def safe_get(session, url):
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = session.get(url, headers=_make_headers(), timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r
        except requests.exceptions.RequestException:
            pass
        time.sleep(2 ** attempt + random.uniform(0.5, 1.5))
    return None

# ============================================================
# WORKER FETCH
# ============================================================

def fetch_worker(session, url):
    try:
        r = session.get(WORKER_URL + url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return r
    except:
        pass
    return None

# ============================================================
# VALIDATION
# ============================================================

def is_blocked(html):
    return "cf-challenge" in html or "Just a moment" in html or len(html) < 5000

def is_valid(html):
    return "ship_lat" in html or "vessel-info-table" in html or "AIS Position" in html

# ============================================================
# HELPERS (UNCHANGED)
# ============================================================

def parse_vf_timestamp(ts):
    if not ts:
        return None
    ts = ts.replace(" UTC", "").strip()
    for fmt in ("%b %d, %Y %H:%M", "%B %d, %Y %H:%M"):
        try:
            return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
        except:
            pass
    return None

def get_vf_age_minutes(ts):
    dt = parse_vf_timestamp(ts)
    if not dt:
        return 999
    return int((datetime.now(timezone.utc) - dt).total_seconds() / 60)

# ============================================================
# MST (IMPROVED)
# ============================================================

def get_myshiptracking_pos(mmsi, lat, lon, session):
    if not mmsi or lat is None:
        return None

    time.sleep(random.uniform(1.0, 2.5))

    params = {
        "type": "json",
        "minlat": lat - 1, "maxlat": lat + 1,
        "minlon": lon - 1, "maxlon": lon + 1,
    }

    try:
        r = session.get(MYSHIPTRACKING_URL, params=params, headers=_make_headers(), timeout=10)
        if r.status_code != 200:
            return None

        for line in r.text.splitlines():
            if mmsi in line:
                parts = line.split()
                return {
                    "lat": float(parts[4]),
                    "lon": float(parts[5]),
                }
    except:
        pass

    return None

# ============================================================
# FETCH PAGE (NEW CORE)
# ============================================================

def fetch_vf_page(session, url):
    time.sleep(random.uniform(2.5, 6.5))
    if random.random() < 0.15:
        time.sleep(random.uniform(8, 12))

    r = safe_get(session, url)

    if r and not is_blocked(r.text) and is_valid(r.text):
        return r.text

    logger.warning("Direct failed → worker")

    r = fetch_worker(session, url)

    if r and not is_blocked(r.text) and is_valid(r.text):
        return r.text

    raise Exception("Blocked")

# ============================================================
# MAIN SCRAPER (YOUR LOGIC KEPT)
# ============================================================

def scrape_vf_full(imo, session):
    url = f"https://www.vesselfinder.com/vessels/details/{imo}"

    html = fetch_vf_page(session, url)
    soup = BeautifulSoup(html, "html.parser")

    name_el = soup.select_one("h1.title")
    name = name_el.get_text(strip=True) if name_el else imo

    djson = soup.find("div", id="djson")

    lat = lon = sog = cog = None

    if djson and djson.has_attr("data-json"):
        data = json.loads(djson["data-json"])
        lat = float(data.get("ship_lat")) if data.get("ship_lat") else None
        lon = float(data.get("ship_lon")) if data.get("ship_lon") else None
        sog = data.get("ship_sog")
        cog = data.get("ship_cog")

    return {
        "imo": imo,
        "vessel_name": name,
        "lat": lat,
        "lon": lon,
        "sog": sog,
        "cog": cog,
    }

# ============================================================
# FASTAPI
# ============================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/vessel/{imo}")
def vessel(imo: str):
    session = create_session()
    try:
        return scrape_vf_full(imo, session)
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(502, "Scrape failed")

@app.post("/vessel-batch")
def vessel_batch(body: dict):
    session = create_session()

    results = {}

    for i, imo in enumerate(body.get("imos", [])):
        results[imo] = scrape_vf_full(imo, session)

        if i % BATCH_SIZE == 0:
            time.sleep(BATCH_COOLDOWN)

    return results

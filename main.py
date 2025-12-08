import json
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any
import re

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


# ------------------------
# Extract MMSI from tables
# ------------------------
def extract_table_mmsi(static_data: Dict[str, str]):
    for key, value in static_data.items():
        if "MMSI" in key.upper():
            v = value.strip()
            return v or None
    return None


# ------------------------
# Extract MMSI from inline JS
# ------------------------
def extract_mmsi_from_js(soup: BeautifulSoup):
    scripts = soup.find_all("script")
    for s in scripts:
        if s.string and "MMSI" in s.string:
            m = re.search(r"MMSI\s*=\s*(\d+)", s.string)
            if m:
                return m.group(1)
    return None


# ------------------------
# Extract table values
# ------------------------
def extract_table_data(soup: BeautifulSoup, table_class: str) -> Dict[str, str]:
    data = {}
    tables = soup.find_all(class_=table_class)

    for table in tables:
        for row in table.find_all("tr"):
            label_el = row.find(class_=lambda x: x and ("tpc1" in x or "tpx1" in x))
            value_el = row.find(class_=lambda x: x and ("tpc2" in x or "tpx2" in x))

            if label_el and value_el:
                label_parts = [c.strip() for c in label_el.contents if isinstance(c, str)]
                label = " ".join(label_parts).replace(":", "").strip()
                data[label] = value_el.get_text(strip=True)

    return data


# -----------------

# app/main.py (or wherever your routes are)
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException

app = FastAPI()

VF_BASE = "https://www.vesselfinder.com/vessels/details"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.vesselfinder.com/",
}

def scrape_vf_full(imo: str) -> dict:
    url = f"{VF_BASE}/{imo}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code == 404:
        return {"found": False, "imo": imo}

    if not r.ok:
        raise HTTPException(status_code=502, detail=f"Vesselfinder HTTP {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")

    # name
    title = soup.find("h1")
    name = title.text.strip() if title else f"IMO {imo}"

    # ---- THIS PART MUST MATCH THE REAL PAGE ----
    # Example if you already extracted JSON earlier:
    # look for application/ld+json, or for a script containing AIS data
    json_ld = soup.find("script", {"type": "application/ld+json"})
    if not json_ld:
        return {
            "found": True,
            "imo": imo,
            "name": name,
            "lat": None,
            "lon": None,
            "sog": None,
            "cog": None,
            "last_pos_utc": None,
            "destination": "",
        }

    data = json.loads(json_ld.text)
    if isinstance(data, list) and data:
        data = data[0]

    lat = float(data.get("latitude")) if data.get("latitude") is not None else None
    lon = float(data.get("longitude")) if data.get("longitude") is not None else None
    sog = float(data.get("speed")) if data.get("speed") is not None else None
    cog = float(data.get("course")) if data.get("course") is not None else None
    last_pos_utc = data.get("dateModified")
    destination = data.get("arrivalDestination", "") or ""

    return {
        "found": True,
        "imo": imo,
        "name": name,
        "lat": lat,
        "lon": lon,
        "sog": sog,
        "cog": cog,
        "last_pos_utc": last_pos_utc,
        "destination": destination,
    }

@app.get("/vessel-full/{imo}")
def vessel_full(imo: str):
    data = scrape_vf_full(imo)
    if not data.get("found"):
        raise HTTPException(status_code=404, detail="Vessel not found")
    return data

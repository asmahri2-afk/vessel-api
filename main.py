import json
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException

app = FastAPI()

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
    url = f"https://www.vesselfinder.com/vessels/details/{imo}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code == 404:
        return {"found": False, "imo": imo}
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # Vessel name
    name_el = soup.select_one("h1.title")
    name = name_el.get_text(strip=True) if name_el else f"IMO {imo}"

    # Destination text
    dest_el = soup.select_one("div.vi__r1.vi__sbt a._npNa")
    destination = dest_el.get_text(strip=True) if dest_el else ""

    # AIS time (from info icon)
    info_icon = soup.select_one("svg.ttt1.info")
    last_pos_utc = (
        info_icon["data-title"]
        if info_icon is not None and info_icon.has_attr("data-title")
        else None
    )

    # AIS numeric data from #djson[data-json]
    djson_div = soup.find("div", id="djson")
    if not djson_div or not djson_div.has_attr("data-json"):
        # no live position → mark as not found
        return {
            "found": True,
            "imo": imo,
            "name": name,
            "lat": None,
            "lon": None,
            "sog": None,
            "cog": None,
            "last_pos_utc": last_pos_utc,
            "destination": destination,
        }

    ais = json.loads(djson_div["data-json"])

    lat = ais.get("ship_lat")
    lon = ais.get("ship_lon")
    sog = ais.get("ship_sog")
    cog = ais.get("ship_cog")

    return {
        "found": True,
        "imo": imo,
        "name": name,
        "lat": lat,
        "lon": lon,
        "sog": sog,
        "cog": cog,
        "last_pos_utc": last_pos_utc,   # e.g. "Nov 30, 2025 17:07 UTC"
        "destination": destination,     # e.g. "Tan Tan, Morocco"
    }


@app.get("/vessel-full/{imo}")
def vessel_full(imo: str):
    data = scrape_vf_full(imo)
    if not data.get("found"):
        raise HTTPException(status_code=404, detail="Vessel not found")
    return data

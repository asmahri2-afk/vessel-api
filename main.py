from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import requests
from bs4 import BeautifulSoup

app = FastAPI()

# --- CORS so your local HTML / GitHub Pages can call this ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # later you can restrict to your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

VF_BASE = "https://www.vesselfinder.com/vessels/details/"


def fetch_vessel_name(imo: str) -> str | None:
    """Scrape VesselFinder page and return vessel name, or None if not found."""
    url = f"{VF_BASE}{imo}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        )
    }

    resp = requests.get(url, headers=headers, timeout=15)

    # 404 or similar
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # On your sample page:  <h1 class="title">DURA BULK</h1>
    h1 = soup.select_one("h1.title") or soup.find("h1")
    if not h1:
        return None

    name = h1.get_text(strip=True)
    return name or None


@app.get("/vessel/{imo}")
def get_vessel(imo: str):
    """
    Simple API: returns vessel name for an IMO, based on VesselFinder.
    Example response:
      { "found": true, "imo": "7325461", "name": "DURA BULK" }
    """
    # Basic sanity check
    if not imo.isdigit():
        raise HTTPException(status_code=400, detail="IMO must be numeric")

    name = fetch_vessel_name(imo)
    if not name:
        return {"found": False, "imo": imo, "name": None}

    return {"found": True, "imo": imo, "name": name}

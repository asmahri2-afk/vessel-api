from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import requests
from bs4 import BeautifulSoup

app = FastAPI()

# Allow your HTML frontend to access the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # allow all, you can restrict later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "ok", "message": "Vessel API is running"}


def fetch_vessel_name(imo: str):
    url = f"https://www.vesselfinder.com/vessels/details/{imo}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        )
    }

    resp = requests.get(url, headers=headers, timeout=10)

    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # VesselFinder title is <h1 class="title">NAME</h1>
    h1 = soup.select_one("h1.title")
    if not h1:
        return None

    return h1.get_text(strip=True)


@app.get("/vessel/{imo}")
def vessel_lookup(imo: str):
    if not imo.isdigit():
        raise HTTPException(status_code=400, detail="IMO must be numeric")

    name = fetch_vessel_name(imo)
    if not name:
        return {"found": False, "imo": imo, "name": None}

    return {"found": True, "imo": imo, "name": name}

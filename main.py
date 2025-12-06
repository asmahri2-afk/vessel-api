import json,re,html,httpx
from fastapi import FastAPI,HTTPException,Request
from fastapi.middleware.cors import CORSMiddleware

HEADERS={
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language":"en-US,en;q=0.9",
    "Referer":"https://www.vesselfinder.com/",
}

app=FastAPI()

@app.get("/ping")
async def ping():
    return{"ok":True}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_cors_headers(req:Request,call_next):
    r=await call_next(req)
    r.headers["Access-Control-Allow-Origin"]="*"
    r.headers["Access-Control-Allow-Methods"]="GET,OPTIONS"
    r.headers["Access-Control-Allow-Headers"]="*"
    return r

async def scrape_vf_full(imo:str)->dict:
    url=f"https://www.vesselfinder.com/vessels/details/{imo}"
    async with httpx.AsyncClient(timeout=20) as c:
        r=await c.get(url,headers=HEADERS)
    if r.status_code==404:
        return{"found":False,"imo":imo}
    r.raise_for_status()
    t=r.text

    m=re.search(r'<h1[^>]*class="title"[^>]*>([^<]+)</h1>',t)
    name=m.group(1).strip() if m else f"IMO {imo}"

    m=re.search(r'<a[^>]*class="_npNa"[^>]*>([^<]+)</a>',t)
    dest=m.group(1).strip() if m else""

    m=re.search(r'class="ttt1 info"[^>]*data-title="([^"]+)"',t)
    last=m.group(1) if m else None

    m=re.search(r'id="djson"[^>]*data-json="([^"]+)"',t)
    if not m:
        return{
            "found":True,
            "imo":imo,
            "name":name,
            "lat":None,"lon":None,
            "sog":None,"cog":None,
            "last_pos_utc":last,
            "destination":dest,
        }

    raw=html.unescape(m.group(1))
    try:
        a=json.loads(raw)
    except:
        return{
            "found":True,
            "imo":imo,
            "name":name,
            "lat":None,"lon":None,
            "sog":None,"cog":None,
            "last_pos_utc":last,
            "destination":dest,
        }

    return{
        "found":True,
        "imo":imo,
        "name":name,
        "lat":a.get("ship_lat"),
        "lon":a.get("ship_lon"),
        "sog":a.get("ship_sog"),
        "cog":a.get("ship_cog"),
        "last_pos_utc":last,
        "destination":dest,
    }

@app.get("/vessel-full/{imo}")
async def vessel_full(imo:str):
    d=await scrape_vf_full(imo)
    if not d.get("found"):
        raise HTTPException(status_code=404,detail="Vessel not found")
    return d

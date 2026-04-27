import time
import asyncio
import json
import websockets
from supabase import create_client

# --- CONFIGURATION ---
S_URL = "YOUR_SUPABASE_URL"
S_KEY = "YOUR_SUPABASE_KEY"
AIS_API_KEY = "YOUR_AISSTREAM_KEY"
UPDATE_INTERVAL = 300  # Set to 180 for 3 min, or 60 for 1 min

supabase = create_client(S_URL, S_KEY)
update_buffer = {} 

def repair_missing_mmsis():
    """
    Checks the vessels table for NULL MMSIs and attempts to 
    recover them from the static_vessel_cache.
    """
    print("Running self-healing check...")
    missing = supabase.table("vessels").select("imo").is_("mmsi", "null").execute()
    
    for row in missing.data:
        imo = row['imo']
        cache = supabase.table("static_vessel_cache").select("mmsi").eq("imo", imo).execute()
        
        if cache.data:
            mmsi = cache.data[0]['mmsi']
            supabase.table("vessels").update({"mmsi": mmsi}).eq("imo", imo).execute()
            print(f"Fixed: Vessel {imo} assigned MMSI {mmsi} from cache.")

async def sync_task():
    """Pushes all buffered updates to Supabase as one batch."""
    global update_buffer
    while True:
        await asyncio.sleep(UPDATE_INTERVAL)
        
        if not update_buffer:
            continue
        
        # Prepare the batch for upsert
        batch = [
            {
                "mmsi": int(m), 
                "lat": d['lat'], 
                "lon": d['lon'], 
                "sog": d['sog'], 
                "cog": d['cog'], 
                "last_update": "now()"
            }
            for m, d in update_buffer.items()
        ]
        
        try:
            # Upsert handles the update via the Unique MMSI index
            supabase.table("vessels").upsert(batch).execute()
            print(f"[{time.strftime('%H:%M:%S')}] Sync Success: {len(batch)} vessels updated.")
            update_buffer = {} # Clear buffer after successful push
        except Exception as e:
            print(f"Sync Error: {e}")

async def listen_ais():
    """Connects to AISStream and captures the latest vessel positions."""
    global update_buffer
    
    # Refresh watchlist from DB (those with MMSIs)
    res = supabase.table("vessels").select("mmsi").not_.is_("mmsi", "null").execute()
    mmsis = [str(r['mmsi']) for r in res.data]

    if not mmsis:
        print("Watchlist empty. Waiting...")
        return

    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
        subscribe = {
            "APIKey": AIS_API_KEY,
            "BoundingBoxes": [[[-90, -180], [90, 180]]], # Global coverage
            "FiltersShipMMSI": mmsis,
            "FilterMessageTypes": ["PositionReport"]
        }
        await ws.send(json.dumps(subscribe))

        async for msg in ws:
            msg_json = json.loads(msg)
            m = msg_json['MetaData']['MMSI']
            p = msg_json['Message']['PositionReport']
            
            # Local buffer: Newest message for the same MMSI always overrides
            update_buffer[m] = {
                "lat": p['Latitude'], 
                "lon": p['Longitude'], 
                "sog": p['Sog'], 
                "cog": p['Cog']
            }

async def main():
    # 1. Run the self-healing repair once at startup
    repair_missing_mmsis()
    
    # 2. Run the listener and the syncer concurrently
    await asyncio.gather(listen_ais(), sync_task())

if __name__ == "__main__":
    asyncio.run(main())

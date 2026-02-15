import os
import asyncio
import pandas as pd
from fastapi import FastAPI
from kiteconnect import KiteConnect, KiteTicker
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import threading
from collections import defaultdict, deque
from supabase import create_client

# ==============================
# CONFIG
# ==============================

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

INDEX = "NIFTY"
STRIKE_RANGE = 800
MAX_CONTRACTS = 80
UPDATE_INTERVAL = 3  # 🔥 3 seconds

# ==============================
# INIT
# ==============================

app = FastAPI()

kite = KiteConnect(api_key=KITE_API_KEY)
kite.set_access_token(KITE_ACCESS_TOKEN)

kws = KiteTicker(KITE_API_KEY, KITE_ACCESS_TOKEN)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

latest_ticks = {}
instrument_df = None
tracked_tokens = []

# In-memory rolling volume store
volume_memory = defaultdict(lambda: deque(maxlen=200))

# ==============================
# MARKET HOURS
# ==============================

def market_open():
    now = datetime.now(ZoneInfo("Asia/Kolkata"))

    if now.weekday() >= 5:
        return False

    if now.hour < 9 or (now.hour == 9 and now.minute < 15):
        return False

    if now.hour > 15 or (now.hour == 15 and now.minute > 30):
        return False

    return True

# ==============================
# LOAD INSTRUMENTS
# ==============================

def load_instruments():
    global instrument_df, tracked_tokens

    instruments = pd.DataFrame(kite.instruments("NFO"))

    df = instruments[
        (instruments["name"] == INDEX) &
        (instruments["segment"].str.contains("OPT"))
    ]

    expiry = sorted(df["expiry"].unique())[0]
    df = df[df["expiry"] == expiry]

    index_price = kite.ltp("NSE:NIFTY 50")
    price = list(index_price.values())[0]["last_price"]

    df = df[
        (df["strike"] > price - STRIKE_RANGE) &
        (df["strike"] < price + STRIKE_RANGE)
    ].head(MAX_CONTRACTS)

    instrument_df = df
    tracked_tokens = df["instrument_token"].tolist()

    print(f"Loaded {len(tracked_tokens)} contracts")

# ==============================
# WEBSOCKET EVENTS
# ==============================

def on_ticks(ws, ticks):
    now = datetime.now(ZoneInfo("Asia/Kolkata"))

    for tick in ticks:
        token = tick["instrument_token"]
        latest_ticks[token] = tick

        # store rolling volume in memory
        volume_memory[token].append((now, tick.get("volume", 0)))

def on_connect(ws, response):
    print("WebSocket connected")
    ws.subscribe(tracked_tokens)
    ws.set_mode(ws.MODE_FULL, tracked_tokens)

def on_close(ws, code, reason):
    print("WebSocket closed:", reason)

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# ==============================
# ROLLING VOLUME CALCULATION
# ==============================

def calculate_volume(token, seconds):
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    total = 0

    for t, v in volume_memory[token]:
        if t >= now - timedelta(seconds=seconds):
            total += v

    return total

# ==============================
# MAIN ENGINE (3 SECOND LOOP)
# ==============================

async def ranking_engine():

    while True:
        try:
            if market_open() and instrument_df is not None:

                now_time = datetime.now(ZoneInfo("Asia/Kolkata"))

                volume_rows = []
                snapshot_rows = []

                for _, row in instrument_df.iterrows():

                    token = row["instrument_token"]
                    tick = latest_ticks.get(token)

                    if not tick:
                        continue

                    current_volume = tick.get("volume", 0)

                    # ======================
                    # STORE VOLUME HISTORY
                    # ======================

                    volume_rows.append({
                        "token": str(token),
                        "volume": current_volume,
                        "created_at": now_time.isoformat()
                    })

                    # ======================
                    # ROLLING VOLUME
                    # ======================

                    vol_10s = calculate_volume(token, 10)
                    vol_30s = calculate_volume(token, 30)
                    vol_1m = calculate_volume(token, 60)
                    vol_3m = calculate_volume(token, 180)
                    vol_5m = calculate_volume(token, 300)

                    # ======================
                    # SCORE
                    # ======================

                    score = (
                        (vol_10s * 0.3) +
                        (vol_30s * 0.25) +
                        (vol_1m * 0.2) +
                        (tick.get("oi", 0) * 0.15) +
                        (abs(tick.get("change", 0)) * 0.1)
                    )

                    confidence = round(min(score / 100000, 1) * 100, 2)
                    volume_power = "HIGH" if vol_10s > vol_1m else "NORMAL"

                    snapshot_rows.append({
                        "symbol": row["tradingsymbol"],
                        "strike": row["strike"],
                        "type": row["instrument_type"],
                        "ltp": tick.get("last_price", 0),
                        "volume": current_volume,
                        "oi": tick.get("oi", 0),
                        "oi_change": 0,
                        "iv": tick.get("iv", 0),
                        "vol_10s": vol_10s,
                        "vol_30s": vol_30s,
                        "vol_1m": vol_1m,
                        "vol_3m": vol_3m,
                        "vol_5m": vol_5m,
                        "score": score,
                        "confidence": confidence,
                        "volume_power": volume_power,
                        "created_at": now_time.isoformat()
                    })

                # ======================
                # BULK INSERT
                # ======================

                if volume_rows:
                    supabase.table("volume_history").insert(volume_rows).execute()

                if snapshot_rows:
                    supabase.table("option_snapshots").insert(snapshot_rows).execute()
                    print(f"Stored {len(snapshot_rows)} contracts at {now_time}")

        except Exception as e:
            print("Engine error:", e)

        await asyncio.sleep(UPDATE_INTERVAL)

# ==============================
# STARTUP
# ==============================

@app.on_event("startup")
async def startup():

    print("Loading instruments...")
    load_instruments()

    if market_open():
        print("Starting WebSocket...")
        threading.Thread(target=kws.connect, daemon=True).start()
    else:
        print("Market closed.")

    asyncio.create_task(ranking_engine())

# ==============================
# ROUTES
# ==============================

@app.get("/")
def root():
    return {"status": "3s Supabase Scanner Running"}

@app.get("/health")
def health():
    return {"status": "running"}

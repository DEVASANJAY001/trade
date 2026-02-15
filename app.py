import os
import asyncio
import pandas as pd
from fastapi import FastAPI
from kiteconnect import KiteConnect, KiteTicker
from datetime import datetime
import threading
import pytz

# ==============================
# CONFIG
# ==============================

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

INDEX = "NIFTY"
STRIKE_RANGE = 800
MAX_CONTRACTS = 80
MIN_VOLUME = 10000
MIN_OI = 50000

# ==============================
# INIT
# ==============================

app = FastAPI()

kite = KiteConnect(api_key=KITE_API_KEY)
kite.set_access_token(KITE_ACCESS_TOKEN)

kws = KiteTicker(KITE_API_KEY, KITE_ACCESS_TOKEN)

latest_ticks = {}
latest_ranked = []
instrument_df = None
tracked_tokens = []

# ==============================
# MARKET HOURS
# ==============================

def market_open():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)

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

# ==============================
# WEBSOCKET EVENTS
# ==============================

def on_ticks(ws, ticks):
    global latest_ticks
    for tick in ticks:
        latest_ticks[tick["instrument_token"]] = tick

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
# RANKING ENGINE
# ==============================

async def ranking_engine():
    global latest_ranked

    while True:
        try:
            if not market_open():
                await asyncio.sleep(5)
                continue

            rows = []

            for _, row in instrument_df.iterrows():
                token = row["instrument_token"]
                tick = latest_ticks.get(token)

                if not tick:
                    continue

                bid = tick.get("depth", {}).get("buy", [{}])[0].get("price", 0)
                ask = tick.get("depth", {}).get("sell", [{}])[0].get("price", 0)
                spread = ask - bid if bid and ask else 0

                rows.append({
                    "symbol": row["tradingsymbol"],
                    "strike": row["strike"],
                    "type": row["instrument_type"],
                    "ltp": tick.get("last_price", 0),
                    "volume": tick.get("volume", 0),
                    "oi": tick.get("oi", 0),
                    "change": tick.get("change", 0),
                    "spread": spread
                })

            df = pd.DataFrame(rows)

            if df.empty:
                await asyncio.sleep(1)
                continue

            # Liquidity filter
            df = df[
                (df["volume"] > MIN_VOLUME) &
                (df["oi"] > MIN_OI) &
                (df["spread"] < 5)
            ]

            if df.empty:
                await asyncio.sleep(1)
                continue

            # Scoring model
            df["volume_score"] = df["volume"] / df["volume"].max()
            df["oi_score"] = df["oi"] / df["oi"].max()
            df["change_score"] = abs(df["change"]) / (abs(df["change"]).max() or 1)

            df["score"] = (
                df["volume_score"] * 0.4 +
                df["oi_score"] * 0.3 +
                df["change_score"] * 0.3
            )

            df["confidence"] = (df["score"] * 100).round(2)

            latest_ranked = df.sort_values(
                "score", ascending=False
            ).to_dict(orient="records")

        except Exception as e:
            print("Ranking error:", e)

        await asyncio.sleep(1)

# ==============================
# STARTUP
# ==============================

@app.on_event("startup")
async def startup():

    print("Loading instruments...")
    load_instruments()

    print("Starting WebSocket...")
    threading.Thread(target=kws.connect, daemon=True).start()

    asyncio.create_task(ranking_engine())

# ==============================
# ROUTES
# ==============================

@app.get("/")
def root():
    return {"status": "Realtime WebSocket backend running"}

@app.get("/scan")
def scan():
    if not latest_ranked:
        return {"message": "Waiting for live ticks..."}
    return latest_ranked

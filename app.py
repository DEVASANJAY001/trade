import os
import time
import numpy as np
import pandas as pd
from fastapi import FastAPI
from kiteconnect import KiteConnect
from supabase import create_client
from datetime import datetime
import pytz

# ======================
# ENV VARIABLES (Render)
# ======================

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

INDEX = "NIFTY"
STRIKE_RANGE = 800
MAX_CONTRACTS = 80

# ======================
# INIT
# ======================

app = FastAPI()

kite = KiteConnect(api_key=KITE_API_KEY)
kite.set_access_token(KITE_ACCESS_TOKEN)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ======================
# ROOT ROUTE
# ======================

@app.get("/")
def root():
    return {"status": "Backend running"}

# ======================
# MARKET HOURS CHECK
# ======================

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

# ======================
# GET PRICE
# ======================

def get_price():
    q = kite.ltp("NSE:NIFTY 50")
    return list(q.values())[0]["last_price"]

# ======================
# VOLUME SPIKE ENGINE (DB BASED)
# ======================

def update_volume(symbol_key, volume):
    supabase.table("volume_history").insert({
        "token": symbol_key,
        "volume": volume
    }).execute()

def calculate_spike(symbol_key):
    now = datetime.utcnow()
    five_min_ago = now - pd.Timedelta(minutes=5)

    history = supabase.table("volume_history") \
        .select("*") \
        .eq("token", symbol_key) \
        .gte("created_at", five_min_ago.strftime("%Y-%m-%dT%H:%M:%S")) \
        .execute().data

    if len(history) < 2:
        return 0, 0, 0, 0, 0

    df = pd.DataFrame(history)
    df["created_at"] = pd.to_datetime(df["created_at"])
    df = df.sort_values("created_at")

    def window(seconds):
        cutoff = now - pd.Timedelta(seconds=seconds)
        window_df = df[df["created_at"] >= cutoff]
        if len(window_df) >= 2:
            return window_df.iloc[-1]["volume"] - window_df.iloc[0]["volume"]
        return 0

    return (
        window(10),
        window(30),
        window(60),
        window(180),
        window(300),
    )

# ======================
# MAIN OPTION SCAN
# ======================

@app.get("/scan")
def scan_options():

    try:

        if not market_open():
            return {"message": "Market closed"}

        instruments = pd.DataFrame(kite.instruments())

        df = instruments[
            (instruments["name"] == INDEX) &
            (instruments["segment"].str.contains("OPT"))
        ]

        expiry = sorted(df["expiry"].unique())[0]
        df = df[df["expiry"] == expiry]

        price = get_price()

        df = df[
            (df["strike"] > price - STRIKE_RANGE) &
            (df["strike"] < price + STRIKE_RANGE)
        ].head(MAX_CONTRACTS)

        # ✅ FIXED: Use tradingsymbol with exchange prefix
        symbols = ["NFO:" + row["tradingsymbol"] for _, row in df.iterrows()]
        quotes = kite.quote(symbols)

        results = []

        for _, row in df.iterrows():

            symbol_key = "NFO:" + row["tradingsymbol"]
            q = quotes.get(symbol_key, {})

            volume = q.get("volume", 0)

            update_volume(symbol_key, volume)
            v10, v30, v1m, v3m, v5m = calculate_spike(symbol_key)

            results.append({
                "symbol": row["tradingsymbol"],
                "strike": row["strike"],
                "type": row["instrument_type"],
                "ltp": q.get("last_price", 0),
                "volume": volume,
                "oi": q.get("oi", 0),
                "oi_change": q.get("oi", 0) - q.get("oi_day_low", 0),
                "iv": q.get("implied_volatility", 0),
                "vol_10s": v10,
                "vol_30s": v30,
                "vol_1m": v1m,
                "vol_3m": v3m,
                "vol_5m": v5m
            })

        df_final = pd.DataFrame(results)

        if df_final.empty:
            return {"message": "No contracts found"}

        # ======================
        # SCORING
        # ======================

        df_final["score"] = (
            df_final["volume"].rank(pct=True) * 0.2 +
            df_final["oi"].rank(pct=True) * 0.2 +
            df_final["iv"].rank(pct=True) * 0.2 +
            df_final["vol_1m"].rank(pct=True) * 0.4
        )

        df_final["confidence"] = (df_final["score"] * 100).round(2)

        # Save snapshot
        supabase.table("option_snapshots").insert(
            df_final.to_dict(orient="records")
        ).execute()

        return df_final.sort_values("score", ascending=False).to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}

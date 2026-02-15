import os
import time
import numpy as np
import pandas as pd
from fastapi import FastAPI
from kiteconnect import KiteConnect
from supabase import create_client

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
# GET PRICE
# ======================

def get_price():
    q = kite.ltp("NSE:NIFTY 50")
    return list(q.values())[0]["last_price"]

# ======================
# VOLUME SPIKE ENGINE (DB BASED)
# ======================

def update_volume(token, volume):
    supabase.table("volume_history").insert({
        "token": token,
        "volume": volume
    }).execute()

def calculate_spike(token):
    now = pd.Timestamp.utcnow()
    five_min_ago = now - pd.Timedelta(minutes=5)

    history = supabase.table("volume_history") \
        .select("*") \
        .eq("token", token) \
        .gte("created_at", five_min_ago.isoformat()) \
        .execute().data

    if len(history) < 2:
        return 0,0,0,0,0

    df = pd.DataFrame(history).sort_values("created_at")

    def window(seconds):
        cutoff = now - pd.Timedelta(seconds=seconds)
        window_df = df[pd.to_datetime(df["created_at"]) >= cutoff]
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

    tokens = df["instrument_token"].astype(str).tolist()
    quotes = kite.quote(tokens)

    results = []

    for _, row in df.iterrows():
        token = str(row["instrument_token"])
        q = quotes.get(token, {})
        volume = q.get("volume", 0)

        update_volume(token, volume)
        v10, v30, v1m, v3m, v5m = calculate_spike(token)

        results.append({
            "symbol": row["tradingsymbol"],
            "strike": row["strike"],
            "type": row["instrument_type"],
            "ltp": q.get("last_price", 0),
            "volume": volume,
            "oi": q.get("oi", 0),
            "oi_change": q.get("oi_day_high", 0) - q.get("oi_day_low", 0),
            "iv": q.get("implied_volatility", 0),
            "vol_10s": v10,
            "vol_30s": v30,
            "vol_1m": v1m,
            "vol_3m": v3m,
            "vol_5m": v5m
        })

    df_final = pd.DataFrame(results)

    # scoring
    df_final["score"] = (
        df_final["volume"].rank(pct=True) * 0.2 +
        df_final["oi"].rank(pct=True) * 0.2 +
        df_final["iv"].rank(pct=True) * 0.2 +
        df_final["vol_1m"].rank(pct=True) * 0.4
    )

    df_final["confidence"] = df_final["score"] * 100

    # save to Supabase
    supabase.table("option_snapshots").insert(
        df_final.to_dict(orient="records")
    ).execute()

    return df_final.sort_values("score", ascending=False).to_dict(orient="records")

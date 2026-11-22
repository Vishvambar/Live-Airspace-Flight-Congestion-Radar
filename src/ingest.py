from sys import is_remote_debug_enabled
from requests import Response
import os
from datetime import datetime, timezone
from pathlib import Path
import requests
import pandas as pd

OPENSKY_URL= "https://opensky-network.org/api/states/all"
INDIA_BBOX={
    "lamin":6.0,
    "lamax":37.0,
    "lomin":68.0,
    "lomax":97.5,
    "extended":1
}

COLUMNS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category",
]

BRONZE_DIR = Path("data/bronze")

def fetch_opensky_snapshot() -> dict:
    response = requests.get(OPENSKY_URL, params=INDIA_BBOX, timeout=15)
    response.raise_for_status()
    return response.json()

def save_to_bronze(payload: dict) -> Path:
    snapshot_time= payload.get("time")
    states= payload.get("states") or []

    if not states: 
        raise ValueError("OPENSKY returned 0 aircraft states, check api status or BBOX")
        
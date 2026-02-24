"""
EcoBici Snapshot Collector
--------------------------
Se ejecuta vía Google Cloud Scheduler + Cloud Functions.
"""

import os
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Configuración de zona horaria CDMX
CDMX = ZoneInfo("America/Mexico_City")

# Horario operativo EcoBici: 05:00 – 00:30 CDMX
OPEN_FROM_MIN = 5 * 60       # 05:00
CLOSE_AT_MIN  = 0 * 60 + 30  # 00:30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# Configuración de URLs
GBFS_BASE          = "https://gbfs.mex.lyftbikes.com/gbfs/en"
STATION_STATUS_URL = f"{GBFS_BASE}/station_status.json"
STATION_INFO_URL   = f"{GBFS_BASE}/station_information.json"

REQUEST_TIMEOUT = 20
MAX_RETRIES     = 3

def fetch_json(url: str) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.warning("Intento %d/%d fallido para %s: %s", attempt, MAX_RETRIES, url, exc)
            if attempt == MAX_RETRIES: raise
    return {}

def upsert_station_info(cur, stations: list[dict]):
    sql = """
        INSERT INTO station_info (station_id, name, capacity, lat, lon)
        VALUES %s
        ON CONFLICT (station_id) DO UPDATE
            SET name = EXCLUDED.name, capacity = EXCLUDED.capacity,
                lat = EXCLUDED.lat, lon = EXCLUDED.lon;
    """
    rows = [(str(s["station_id"]), s.get("name", ""), s.get("capacity"), s.get("lat"), s.get("lon")) 
            for s in stations if s.get("station_id")]
    if rows: psycopg2.extras.execute_values(cur, sql, rows)

def insert_snapshots(cur, stations: list[dict], collected_at: datetime, origin: str):
    # Usamos la columna 'origin' que ya creaste en Supabase
    sql = """
        INSERT INTO snapshots
            (collected_at, station_id, bikes_available, bikes_disabled,
             docks_available, docks_disabled, is_installed, is_renting, is_returning, origin)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """
    rows = [
        (
            collected_at, str(s["station_id"]),
            s.get("num_bikes_available", 0), s.get("num_bikes_disabled", 0),
            s.get("num_docks_available", 0), s.get("num_docks_disabled", 0),
            bool(s.get("is_installed", 0)), bool(s.get("is_renting", 0)),
            bool(s.get("is_returning", 0)), origin
        )
        for s in stations if s.get("station_id")
    ]
    if rows: psycopg2.extras.execute_values(cur, sql, rows)

def in_operating_hours(ts: datetime) -> bool:
    local = ts.astimezone(CDMX)
    minutes = local.hour * 60 + local.minute
    return not (CLOSE_AT_MIN <= minutes < OPEN_FROM_MIN)

def collect(origin: str = "manual"):
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url: raise RuntimeError("Falta SUPABASE_DB_URL")

    now_utc = datetime.now(tz=timezone.utc)
    if not in_operating_hours(now_utc):
        log.info("Fuera de horario operativo.")
        return f"Fuera de horario (Origin: {origin})", 200

    status_data = fetch_json(STATION_STATUS_URL)
    status_stations = status_data.get("data", {}).get("stations", [])
    
    last_updated = status_data.get("last_updated")
    collected_at = datetime.fromtimestamp(last_updated, tz=timezone.utc) if last_updated else now_utc

    info_data = fetch_json(STATION_INFO_URL)
    info_stations = info_data.get("data", {}).get("stations", [])

    con = psycopg2.connect(db_url)
    try:
        with con:
            with con.cursor() as cur:
                if info_stations: upsert_station_info(cur, info_stations)
                n_snaps = insert_snapshots(cur, status_stations, collected_at, origin)
                log.info(f"Insertados {n_snaps} snapshots (Origin: {origin})")
    finally:
        con.close()
    return f"OK (Origin: {origin})", 200

# --- EL PUENTE ---
def run_collector(request):
    """Detecta quién llama y le pasa la etiqueta a la función collect"""
    ua = request.headers.get('User-Agent', '').lower()
    
    if 'google-cloud-scheduler' in ua:
        origin = 'google-cloud'
    elif 'github' in ua:
        origin = 'github-actions'
    else:
        origin = 'manual'

    try:
        return collect(origin)
    except Exception as exc:
        log.error(f"Error: {exc}")
        return f"ERROR: {exc}", 500

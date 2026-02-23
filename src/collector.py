"""
EcoBici Snapshot Collector
--------------------------
Se ejecuta cada 15 minutos vía GitHub Actions.
Descarga el estado actual de todas las estaciones desde el API GBFS de EcoBici
y persiste los datos en Supabase (PostgreSQL).

Tablas requeridas: snapshots, station_info
Ver docs/supabase_setup.sql para el schema completo.
"""

import os
import sys
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CDMX = ZoneInfo("America/Mexico_City")

# Horario operativo EcoBici: 05:00 – 00:30 CDMX
# Fuera de ese rango (00:30 – 04:59) no hay bicis en circulación
OPEN_FROM_MIN = 5 * 60       # 05:00 → 300 min desde medianoche
CLOSE_AT_MIN  = 0 * 60 + 30  # 00:30 → 30 min desde medianoche

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
DB_URL = os.environ["SUPABASE_DB_URL"]

GBFS_BASE = "https://gbfs.mex.lyftbikes.com/gbfs/en"
STATION_STATUS_URL = f"{GBFS_BASE}/station_status.json"
STATION_INFO_URL   = f"{GBFS_BASE}/station_information.json"

REQUEST_TIMEOUT = 20   # segundos
MAX_RETRIES     = 3


# ---------------------------------------------------------------------------
# Helpers de red
# ---------------------------------------------------------------------------
def fetch_json(url: str) -> dict:
    """Descarga JSON con reintentos simples."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.warning("Intento %d/%d fallido para %s: %s", attempt, MAX_RETRIES, url, exc)
            if attempt == MAX_RETRIES:
                raise
    return {}   # nunca se llega aquí, pero satisface a mypy


# ---------------------------------------------------------------------------
# Lógica de persistencia
# ---------------------------------------------------------------------------
def upsert_station_info(cur, stations: list[dict]) -> int:
    """
    Inserta o actualiza la información estática de cada estación.
    Sólo escribe si cambian nombre o capacidad para no saturar la DB.
    """
    sql = """
        INSERT INTO station_info (station_id, name, capacity, lat, lon)
        VALUES %s
        ON CONFLICT (station_id) DO UPDATE
            SET name     = EXCLUDED.name,
                capacity = EXCLUDED.capacity,
                lat      = EXCLUDED.lat,
                lon      = EXCLUDED.lon;
    """
    rows = [
        (
            str(s["station_id"]),
            s.get("name", ""),
            s.get("capacity"),
            s.get("lat"),
            s.get("lon"),
        )
        for s in stations
        if s.get("station_id")
    ]
    if rows:
        psycopg2.extras.execute_values(cur, sql, rows)
    return len(rows)


def insert_snapshots(cur, stations: list[dict], collected_at: datetime) -> int:
    """Inserta un snapshot por estación en la tabla snapshots."""
    sql = """
        INSERT INTO snapshots
            (collected_at, station_id, bikes_available, docks_available,
             is_installed, is_renting, is_returning)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """
    rows = [
        (
            collected_at,
            str(s["station_id"]),
            s.get("num_bikes_available", 0),
            s.get("num_docks_available", 0),
            bool(s.get("is_installed", 0)),
            bool(s.get("is_renting", 0)),
            bool(s.get("is_returning", 0)),
        )
        for s in stations
        if s.get("station_id")
    ]
    if rows:
        psycopg2.extras.execute_values(cur, sql, rows)
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def in_operating_hours(ts: datetime) -> bool:
    """Devuelve True si el timestamp está dentro del horario operativo (05:00–00:30 CDMX)."""
    local = ts.astimezone(CDMX)
    minutes = local.hour * 60 + local.minute
    # Cerrado: 00:30 (30 min) hasta 05:00 (300 min), exclusive
    return not (CLOSE_AT_MIN <= minutes < OPEN_FROM_MIN)


def collect():
    log.info("Iniciando recolección de snapshots EcoBici...")

    # 0. Verificar horario operativo antes de llamar al API
    now_utc = datetime.now(tz=timezone.utc)
    if not in_operating_hours(now_utc):
        now_cdmx = now_utc.astimezone(CDMX)
        log.info("Fuera de horario operativo (%s CDMX, 00:30–05:00). Sin recolección.", now_cdmx.strftime("%H:%M"))
        sys.exit(0)

    # 1. Descargar status (siempre necesario)
    status_data = fetch_json(STATION_STATUS_URL)
    status_stations = status_data.get("data", {}).get("stations", [])
    if not status_stations:
        log.error("No se recibieron estaciones del API de status. Abortando.")
        sys.exit(1)

    # Usar el timestamp del feed como instante de recolección
    last_updated = status_data.get("last_updated")
    collected_at = (
        datetime.fromtimestamp(last_updated, tz=timezone.utc)
        if last_updated
        else datetime.now(tz=timezone.utc)
    )
    log.info("Feed timestamp: %s | Estaciones en status: %d", collected_at.isoformat(), len(status_stations))

    # 2. Descargar info estática (para mantener station_info actualizada)
    try:
        info_data = fetch_json(STATION_INFO_URL)
        info_stations = info_data.get("data", {}).get("stations", [])
    except requests.RequestException:
        log.warning("No se pudo obtener station_information.json — se omite upsert de info.")
        info_stations = []

    # 3. Persistir en Supabase
    con = psycopg2.connect(DB_URL)
    try:
        with con:
            with con.cursor() as cur:
                # Actualizar info estática de estaciones
                if info_stations:
                    n_info = upsert_station_info(cur, info_stations)
                    log.info("station_info actualizada: %d estaciones", n_info)

                # Insertar snapshots
                n_snaps = insert_snapshots(cur, status_stations, collected_at)
                log.info("Snapshots insertados: %d", n_snaps)
    finally:
        con.close()

    log.info("Recoleccion completada exitosamente.")


if __name__ == "__main__":
    collect()

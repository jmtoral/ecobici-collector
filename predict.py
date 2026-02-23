"""
EcoBici Predict CLI
Uso:
  python predict.py --station 059 --hour 8 --dow 0   # Estación 059, Lunes 8am
  python predict.py --report                          # Top estaciones ahora mismo
"""

import argparse
import os
import pickle
import numpy as np
import pandas as pd
import requests
import psycopg2
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

MODEL_FILE = "ecobici_model.pkl"
DB_URL     = os.environ.get("SUPABASE_DB_URL")
GBFS_STATUS = "https://gbfs.mex.lyftbikes.com/gbfs/en/station_status.json"
CDMX = ZoneInfo("America/Mexico_City")
DIAS = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]


def load_model():
    with open(MODEL_FILE, "rb") as f:
        return pickle.load(f)


def build_row(station_enc, hour, dow, capacity=None):
    row = {
        "station_enc": station_enc,
        "hour_sin":    np.sin(2 * np.pi * hour / 24),
        "hour_cos":    np.cos(2 * np.pi * hour / 24),
        "dow_sin":     np.sin(2 * np.pi * dow  / 7),
        "dow_cos":     np.cos(2 * np.pi * dow  / 7),
        "is_weekend":  int(dow >= 5),
    }
    if capacity is not None:
        row["capacity"] = capacity
    return row


def predict_station(station_id: str, hour: int, dow: int):
    artifact = load_model()
    model, le, features = artifact["model"], artifact["label_encoder"], artifact["features"]

    if station_id not in le.classes_:
        print(f"Estación '{station_id}' no conocida por el modelo.")
        return

    enc = le.transform([station_id])[0]
    capacity = None

    if "capacity" in features and DB_URL:
        con = psycopg2.connect(DB_URL)
        cur = con.cursor()
        cur.execute("SELECT capacity FROM station_info WHERE station_id = %s", [station_id])
        row = cur.fetchone()
        capacity = row[0] if row else 15
        con.close()

    X = pd.DataFrame([build_row(enc, hour, dow, capacity)])[features]
    prob = artifact["model"].predict_proba(X)[0, 1]
    semaforo = "🟢" if prob >= 0.7 else ("🟡" if prob >= 0.4 else "🔴")

    print(f"\n  Estación : {station_id}")
    print(f"  Cuándo   : {DIAS[dow]} {hour:02d}:00 h")
    print(f"  P(≥1 bici): {prob:.1%} {semaforo}\n")


def report():
    artifact = load_model()
    model, le, features = artifact["model"], artifact["label_encoder"], artifact["features"]

    data = requests.get(GBFS_STATUS, timeout=15).json()
    ts   = datetime.fromtimestamp(data["last_updated"], tz=timezone.utc).astimezone(CDMX)
    now_hour = ts.hour
    now_dow  = ts.weekday()

    capacity_map = {}
    if "capacity" in features and DB_URL:
        con = psycopg2.connect(DB_URL)
        info = pd.read_sql("SELECT station_id, capacity FROM station_info", con)
        con.close()
        capacity_map = dict(zip(info.station_id, info.capacity))
        med_cap = info.capacity.median()

    rows = []
    for s in data["data"]["stations"]:
        sid = str(s.get("station_id", ""))
        if sid not in le.classes_:
            continue
        enc = le.transform([sid])[0]
        cap = capacity_map.get(sid, med_cap) if "capacity" in features else None
        row = build_row(enc, now_hour, now_dow, cap)
        rows.append({**row, "station_id": sid, "bikes_now": s.get("num_bikes_available", 0)})

    df = pd.DataFrame(rows)
    X  = df[features]
    df["prob"] = model.predict_proba(X)[:, 1]
    df = df.sort_values("prob", ascending=False)

    print(f"\n{'═'*58}")
    print(f"  EcoBici — {ts.strftime('%Y-%m-%d %H:%M')} ({DIAS[now_dow]})")
    print(f"{'═'*58}")
    print(f"  {'Estación':<12} {'Bicis ahora':>11} {'P(disponible)':>14}  ")
    print(f"{'─'*58}")
    for _, r in df.head(20).iterrows():
        p = r["prob"]
        s = "🟢" if p >= 0.7 else ("🟡" if p >= 0.4 else "🔴")
        print(f"  {r['station_id']:<12} {int(r['bikes_now']):>11} {p:>14.1%}  {s}")
    print(f"{'─'*58}")
    print(f"  Top 20 de {len(df)} estaciones activas")
    print(f"{'═'*58}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", help="ID de estación")
    parser.add_argument("--hour",    type=int, help="Hora (0-23)")
    parser.add_argument("--dow",     type=int, help="Día de semana (0=Lunes, 6=Domingo)")
    parser.add_argument("--report",  action="store_true", help="Reporte de todas las estaciones ahora")
    args = parser.parse_args()

    if args.report:
        report()
    elif args.station and args.hour is not None and args.dow is not None:
        predict_station(args.station, args.hour, args.dow)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

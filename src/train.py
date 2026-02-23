"""
EcoBici Model Trainer
Corre en GitHub Actions una vez por semana.
Lee snapshots de Supabase, entrena el modelo y lo sube como artifact.
"""

import os
import pickle
import psycopg2
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report

DB_URL     = os.environ["SUPABASE_DB_URL"]
MODEL_FILE = "ecobici_model.pkl"


def load_data() -> pd.DataFrame:
    con = psycopg2.connect(DB_URL)
    df = pd.read_sql("""
        SELECT
            s.collected_at,
            s.station_id,
            s.bikes_available,
            s.is_renting,
            si.capacity
        FROM snapshots s
        LEFT JOIN station_info si USING (station_id)
        WHERE s.is_installed = TRUE
        ORDER BY s.collected_at
    """, con)
    con.close()
    print(f"Registros cargados: {len(df):,}")
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ts_local"]   = pd.to_datetime(df["collected_at"]).dt.tz_convert("America/Mexico_City")
    df["hour"]       = df["ts_local"].dt.hour
    df["dow"]        = df["ts_local"].dt.dayofweek

    # Filtrar horario fuera de operación (00:30–05:00 CDMX)
    # para no enseñarle al modelo que "no hay bicis" por cierre del sistema
    minutes = df["hour"] * 60 + df["ts_local"].dt.minute
    df = df[~((minutes >= 30) & (minutes < 300))].copy()
    print(f"Registros en horario operativo (05:00–00:30): {len(df):,}")

    df["is_weekend"] = (df["dow"] >= 5).astype(int)
    df["hour_sin"]   = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"]   = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"]    = np.sin(2 * np.pi * df["dow"]  / 7)
    df["dow_cos"]    = np.cos(2 * np.pi * df["dow"]  / 7)
    df["disponible"] = (df["bikes_available"] >= 1).astype(int)
    return df


def train():
    df = load_data()
    if len(df) < 1000:
        print(f"⚠ Solo {len(df)} registros — el modelo puede ser poco confiable.")

    df = build_features(df)

    le = LabelEncoder()
    df["station_enc"] = le.fit_transform(df["station_id"])

    FEATURES = ["station_enc", "hour_sin", "hour_cos", "dow_sin", "dow_cos", "is_weekend"]

    if df["capacity"].notna().mean() > 0.5:
        df["capacity"] = df["capacity"].fillna(df["capacity"].median())
        FEATURES.append("capacity")

    X, y = df[FEATURES], df["disponible"]
    print(f"Disponibilidad media: {y.mean():.1%}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = GradientBoostingClassifier(
        n_estimators=300, max_depth=4, learning_rate=0.05, random_state=42
    )
    print("Entrenando...")
    model.fit(X_train, y_train)

    y_prob = model.predict_proba(X_test)[:, 1]
    auc    = roc_auc_score(y_test, y_prob)
    print(f"\nAUC-ROC: {auc:.4f}")
    print(classification_report(y_test, (y_prob >= 0.5).astype(int),
                                 target_names=["Sin bici", "Con bici"]))

    artifact = {
        "model":         model,
        "label_encoder": le,
        "features":      FEATURES,
        "trained_at":    pd.Timestamp.now(tz="UTC").isoformat(),
        "n_samples":     len(df),
        "auc":           auc,
    }
    with open(MODEL_FILE, "wb") as f:
        pickle.dump(artifact, f)

    print(f"\n✓ Modelo guardado: {MODEL_FILE}")
    print(f"  Estaciones conocidas: {len(le.classes_)}")
    print(f"  Features: {FEATURES}")


if __name__ == "__main__":
    train()

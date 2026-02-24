"""
EcoBici Dashboard
-----------------
Muestra el estado del pipeline de recolección y un EDA básico
de los datos acumulados en Supabase.

Despliegue: Streamlit Community Cloud
  Main file : app/app.py
  Secrets   : SUPABASE_DB_URL
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import psycopg2
import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Configuración global
# ---------------------------------------------------------------------------
CDMX          = ZoneInfo("America/Mexico_City")
GITHUB_REPO   = "jmtoral/ecobici-collector"
WORKFLOW_FILE = "collect.yml"
DIAS          = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

st.set_page_config(
    page_title="EcoBici Dashboard",
    page_icon="🚲",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Carga de datos (con caché para no saturar Supabase)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)   # refresca cada 5 min
def load_data() -> pd.DataFrame:
    db_url = st.secrets.get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL", "")
    con = psycopg2.connect(db_url)
    df = pd.read_sql("""
        SELECT
            s.collected_at,
            s.station_id,
            s.bikes_available,
            s.bikes_disabled,
            s.docks_available,
            s.docks_disabled,
            s.is_renting,
            COALESCE(si.name, s.station_id) AS station_name,
            si.capacity
        FROM snapshots s
        LEFT JOIN station_info si USING (station_id)
        WHERE s.is_installed = TRUE
        ORDER BY s.collected_at
    """, con)
    con.close()

    df["collected_at"]   = pd.to_datetime(df["collected_at"]).dt.tz_convert(CDMX)
    df["hour"]           = df["collected_at"].dt.hour
    df["dow"]            = df["collected_at"].dt.dayofweek
    df["disponible"]     = (df["bikes_available"] >= 1).astype(int)
    df["bikes_total"]    = df["bikes_available"] + df["bikes_disabled"]
    df["disabled_ratio"] = df["bikes_disabled"] / df["bikes_total"].replace(0, pd.NA)

    # Filtrar horario fuera de operación EcoBici: 00:30–05:00 CDMX
    minutes = df["hour"] * 60 + df["collected_at"].dt.minute
    df = df[~((minutes >= 30) & (minutes < 300))].copy()
    return df


@st.cache_data(ttl=60)    # refresca cada 1 min
def load_workflow_runs() -> list:
    url = (
        f"https://api.github.com/repos/{GITHUB_REPO}"
        f"/actions/workflows/{WORKFLOW_FILE}/runs?per_page=6"
    )
    try:
        resp = requests.get(url, timeout=10, headers={"Accept": "application/vnd.github+json"})
        if resp.status_code == 200:
            return resp.json().get("workflow_runs", [])
    except requests.RequestException:
        pass
    return []


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🚲 EcoBici · Dashboard de recolección")
st.caption(
    f"Datos de disponibilidad de bicicletas en CDMX · "
    f"Actualizado: {datetime.now(CDMX).strftime('%Y-%m-%d %H:%M')} CDMX"
)

# ---------------------------------------------------------------------------
# Carga con spinner
# ---------------------------------------------------------------------------
with st.spinner("Cargando datos desde Supabase..."):
    df   = load_data()
    runs = load_workflow_runs()

# ---------------------------------------------------------------------------
# Métricas principales
# ---------------------------------------------------------------------------
n_rows          = len(df)
n_stations      = df["station_id"].nunique()
n_colectas      = df["collected_at"].nunique()
first_ts        = df["collected_at"].min()
last_ts         = df["collected_at"].max()
span_h          = (last_ts - first_ts).total_seconds() / 3600
pct_disp        = df["disponible"].mean()
pct_disabled    = df["bikes_disabled"].sum() / df["bikes_total"].sum() if df["bikes_total"].sum() > 0 else 0

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Registros totales",       f"{n_rows:,}")
c2.metric("Estaciones activas",      f"{n_stations:,}")
c3.metric("Recolecciones",           f"{n_colectas:,}")
c4.metric("Horas de historial",      f"{span_h:.1f} h")
c5.metric("Disponibilidad media",    f"{pct_disp:.1%}")
c6.metric("🔧 Bicis descompuestas",  f"{pct_disabled:.1%}", delta="del total en circulación", delta_color="inverse")

st.divider()

# ---------------------------------------------------------------------------
# Estado del pipeline · GitHub Actions
# ---------------------------------------------------------------------------
st.subheader("Estado del pipeline · últimas ejecuciones de collect.yml")

STATUS = {
    "success":     ("✅", "normal"),
    "failure":     ("❌", "inverse"),
    "cancelled":   ("⛔", "inverse"),
    "in_progress": ("🔄", "off"),
    "queued":      ("⏳", "off"),
}

if runs:
    cols = st.columns(len(runs))
    for col, run in zip(cols, runs):
        conclusion = run.get("conclusion") or run.get("status", "queued")
        icon, delta_color = STATUS.get(conclusion, ("❓", "off"))
        ts_utc  = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        ts_cdmx = ts_utc.astimezone(CDMX).strftime("%d/%m %H:%M")
        dur_s   = run.get("run_duration_ms", 0) // 1000
        dur_str = f"{dur_s}s" if dur_s else "—"
        col.metric(
            label=f"{icon} Run #{run['run_number']}",
            value=ts_cdmx,
            delta=f"{conclusion} · {dur_str}",
            delta_color=delta_color,
        )
else:
    st.info("No se pudo obtener el estado de GitHub Actions (repo público requerido).")

st.divider()

# ---------------------------------------------------------------------------
# EDA · Disponibilidad
# ---------------------------------------------------------------------------
st.subheader("Exploración de datos · Disponibilidad")

# ── Fila 1: distribución de bicis + disponibilidad por hora ─────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("**Distribución de bicis disponibles por snapshot**")
    fig_hist = px.histogram(
        df, x="bikes_available", nbins=35,
        color_discrete_sequence=["#27ae60"],
        labels={"bikes_available": "Bicis disponibles", "count": "Frecuencia"},
    )
    fig_hist.update_layout(margin=dict(t=5, b=5), height=320, bargap=0.05)
    st.plotly_chart(fig_hist, use_container_width=True)

with col_b:
    st.markdown("**Tasa de disponibilidad por hora del día**")
    hourly = (
        df.groupby("hour")["disponible"]
        .mean()
        .reset_index()
        .rename(columns={"disponible": "P(disponible)"})
    )
    fig_hora = px.bar(
        hourly, x="hour", y="P(disponible)",
        color="P(disponible)", color_continuous_scale="RdYlGn",
        range_color=[0, 1],
        labels={"hour": "Hora del día", "P(disponible)": "P(≥1 bici)"},
    )
    fig_hora.update_layout(
        margin=dict(t=5, b=5), height=320,
        coloraxis_showscale=False,
        xaxis=dict(tickmode="linear", tick0=0, dtick=2),
    )
    st.plotly_chart(fig_hora, use_container_width=True)

# ── Heatmap hora × día ───────────────────────────────────────────────────────
st.markdown("**Disponibilidad media por hora y día de semana**")
heat = df.groupby(["dow", "hour"])["disponible"].mean().reset_index()
heat["dow_name"] = heat["dow"].map(dict(enumerate(DIAS)))
pivot = heat.pivot(index="dow_name", columns="hour", values="disponible")
pivot = pivot.reindex([d for d in DIAS if d in pivot.index])

fig_heat = px.imshow(
    pivot,
    color_continuous_scale="RdYlGn",
    zmin=0, zmax=1,
    labels={"x": "Hora", "y": "Día", "color": "P(≥1 bici)"},
    aspect="auto",
    text_auto=".0%",
)
fig_heat.update_traces(textfont_size=10)
fig_heat.update_layout(margin=dict(t=5, b=5), height=300)
st.plotly_chart(fig_heat, use_container_width=True)

# ── Top / Bottom estaciones ───────────────────────────────────────────────────
st.markdown("**Estaciones por tasa de disponibilidad** *(mín. 2 observaciones)*")

station_stats = (
    df.groupby(["station_id", "station_name"])["disponible"]
    .agg(disponibilidad="mean", observaciones="count")
    .reset_index()
    .query("observaciones >= 2")
    .sort_values("disponibilidad", ascending=False)
)

tab_top, tab_bot = st.tabs(["Top 15 · más disponibles", "Bottom 15 · menos disponibles"])

with tab_top:
    top15 = station_stats.head(15)
    fig_top = px.bar(
        top15, x="disponibilidad", y="station_name", orientation="h",
        color="disponibilidad", color_continuous_scale="Greens",
        range_color=[0.5, 1],
        text=top15["disponibilidad"].map("{:.0%}".format),
        labels={"disponibilidad": "P(≥1 bici)", "station_name": "Estación"},
    )
    fig_top.update_traces(textposition="outside")
    fig_top.update_layout(
        yaxis=dict(autorange="reversed"), margin=dict(t=5, b=5),
        height=420, coloraxis_showscale=False,
    )
    st.plotly_chart(fig_top, use_container_width=True)

with tab_bot:
    bot15 = station_stats.tail(15).sort_values("disponibilidad")
    fig_bot = px.bar(
        bot15, x="disponibilidad", y="station_name", orientation="h",
        color="disponibilidad", color_continuous_scale="Reds_r",
        range_color=[0, 0.5],
        text=bot15["disponibilidad"].map("{:.0%}".format),
        labels={"disponibilidad": "P(≥1 bici)", "station_name": "Estación"},
    )
    fig_bot.update_traces(textposition="outside")
    fig_bot.update_layout(
        yaxis=dict(autorange="reversed"), margin=dict(t=5, b=5),
        height=420, coloraxis_showscale=False,
    )
    st.plotly_chart(fig_bot, use_container_width=True)

# ── Timeline disponibilidad ──────────────────────────────────────────────────
if n_colectas > 1:
    st.markdown("**Evolución: promedio de bicis disponibles por recolección**")
    timeline = (
        df.groupby("collected_at")["bikes_available"]
        .mean()
        .reset_index()
        .rename(columns={"bikes_available": "bicis_promedio"})
    )
    fig_time = px.line(
        timeline, x="collected_at", y="bicis_promedio",
        color_discrete_sequence=["#2980b9"],
        labels={"collected_at": "", "bicis_promedio": "Bicis disponibles (promedio)"},
    )
    fig_time.update_layout(margin=dict(t=5, b=5), height=280)
    st.plotly_chart(fig_time, use_container_width=True)
else:
    st.info("La línea de tiempo aparecerá cuando haya más de una recolección.")

st.divider()

# ---------------------------------------------------------------------------
# Sección nueva · Bicis descompuestas 🔧
# ---------------------------------------------------------------------------
st.subheader("🔧 Análisis de bicis descompuestas")

# ── Métrica de contexto ──────────────────────────────────────────────────────
total_disabled  = int(df["bikes_disabled"].sum())
avg_disabled    = df["bikes_disabled"].mean()
worst_snapshot  = df.groupby("collected_at")["bikes_disabled"].sum().max()

m1, m2, m3 = st.columns(3)
m1.metric("Total registros con bici descompuesta", f"{total_disabled:,}")
m2.metric("Promedio descompuestas por snapshot",   f"{avg_disabled:.2f}")
m3.metric("Pico máximo en una recolección",        f"{int(worst_snapshot):,}")

# ── Timeline de descomposturas ───────────────────────────────────────────────
if n_colectas > 1:
    st.markdown("**Evolución de bicis descompuestas a lo largo del tiempo**")
    tl_disabled = (
        df.groupby("collected_at")[["bikes_disabled", "bikes_available"]]
        .mean()
        .reset_index()
        .rename(columns={
            "bikes_disabled":  "Descompuestas (prom)",
            "bikes_available": "Disponibles (prom)",
        })
    )
    fig_tl_dis = px.line(
        tl_disabled.melt(id_vars="collected_at", var_name="tipo", value_name="bicis"),
        x="collected_at", y="bicis", color="tipo",
        color_discrete_map={
            "Descompuestas (prom)": "#e74c3c",
            "Disponibles (prom)":   "#2980b9",
        },
        labels={"collected_at": "", "bicis": "Bicis (promedio por estación)", "tipo": ""},
    )
    fig_tl_dis.update_layout(margin=dict(t=5, b=5), height=300)
    st.plotly_chart(fig_tl_dis, use_container_width=True)
else:
    st.info("La línea de tiempo aparecerá cuando haya más de una recolección.")

# ── Ranking de estaciones con más descomposturas ─────────────────────────────
st.markdown("**Estaciones con mayor tasa de bicis descompuestas** *(mín. 2 observaciones)*")

disabled_stats = (
    df.groupby(["station_id", "station_name"])
    .agg(
        avg_disabled=("bikes_disabled", "mean"),
        avg_total=("bikes_total", "mean"),
        observaciones=("bikes_disabled", "count"),
    )
    .reset_index()
    .query("observaciones >= 2")
)
disabled_stats["pct_disabled"] = (
    disabled_stats["avg_disabled"] / disabled_stats["avg_total"].replace(0, pd.NA)
).fillna(0)

tab_abs, tab_pct = st.tabs([
    "Top 15 · más descompuestas (promedio absoluto)",
    "Top 15 · mayor % de flota descompuesta",
])

with tab_abs:
    top_abs = disabled_stats.nlargest(15, "avg_disabled")
    fig_abs = px.bar(
        top_abs, x="avg_disabled", y="station_name", orientation="h",
        color="avg_disabled", color_continuous_scale="Reds",
        text=top_abs["avg_disabled"].map("{:.1f}".format),
        labels={"avg_disabled": "Descompuestas (prom)", "station_name": "Estación"},
    )
    fig_abs.update_traces(textposition="outside")
    fig_abs.update_layout(
        yaxis=dict(autorange="reversed"), margin=dict(t=5, b=5),
        height=420, coloraxis_showscale=False,
    )
    st.plotly_chart(fig_abs, use_container_width=True)

with tab_pct:
    top_pct = disabled_stats.nlargest(15, "pct_disabled")
    fig_pct = px.bar(
        top_pct, x="pct_disabled", y="station_name", orientation="h",
        color="pct_disabled", color_continuous_scale="OrRd",
        range_color=[0, 1],
        text=top_pct["pct_disabled"].map("{:.0%}".format),
        labels={"pct_disabled": "% flota descompuesta", "station_name": "Estación"},
    )
    fig_pct.update_traces(textposition="outside")
    fig_pct.update_layout(
        yaxis=dict(autorange="reversed"), margin=dict(t=5, b=5),
        height=420, coloraxis_showscale=False,
    )
    st.plotly_chart(fig_pct, use_container_width=True)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.divider()
st.caption(
    "Fuente: [GBFS API EcoBici](https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json) · "
    "[github.com/jmtoral/ecobici-collector](https://github.com/jmtoral/ecobici-collector)"
)

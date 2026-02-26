# EcoBici Availability Predictor

Pipeline automatizado para recolectar datos de disponibilidad de bicicletas en **EcoBici (CDMX)** y entrenar un modelo de clasificación que estima la **probabilidad de encontrar al menos una bicicleta funcional** en una estación dada, a cualquier hora del día y día de la semana.

---

## Tabla de contenidos

1. [¿Qué hace este proyecto?](#qué-hace-este-proyecto)
2. [Arquitectura](#arquitectura)
3. [Estructura del proyecto](#estructura-del-proyecto)
4. [¿Por qué Google Cloud en vez de GitHub Actions?](#por-qué-google-cloud-en-vez-de-github-actions)
5. [Scheduling y cron](#scheduling-y-cron)
6. [¿Por qué Supabase?](#por-qué-supabase)
7. [¿Por qué 15 minutos?](#por-qué-15-minutos)
8. [Costos y límites gratuitos](#costos-y-límites-gratuitos)
9. [El modelo](#el-modelo)
10. [Setup paso a paso](#setup-paso-a-paso)
11. [Uso del CLI de predicción](#uso-del-cli-de-predicción)
12. [Dashboard](#dashboard)
13. [Dependencias](#dependencias)

---

## ¿Qué hace este proyecto?

EcoBici expone el estado de sus estaciones en tiempo real a través de un API público en formato [GBFS](https://gbfs.org/) (General Bikeshare Feed Specification). Cada llamada devuelve, para cada estación, cuántas bicis hay disponibles, cuántas están descompuestas y cuántos espacios libres existen.

Este proyecto:

1. **Recolecta** ese estado cada 15 minutos, de forma automática, y lo guarda en una base de datos.
2. **Entrena** semanalmente un modelo de machine learning con los datos acumulados.
3. **Predice** la probabilidad de que una estación tenga al menos una bici disponible, dado el día de la semana y la hora.

La pregunta que responde el modelo es simple: _"Si voy a la estación X un martes a las 8am, ¿qué tan probable es que haya bici?"_

---

## Arquitectura

```mermaid
flowchart TD
    API["GBFS API\ngbfs.mex.lyftbikes.com"]

    subgraph GCP["Google Cloud Platform"]
        SCH["Cloud Scheduler\ncada 15 min"]
    end

    subgraph SB["Supabase"]
        EF["Edge Function\nrun_collector()"]
        SI["station_info\nnombre, capacidad, lat/lon"]
        SN["snapshots\nbikes_available, bikes_disabled\norigin: google-cloud | github-actions"]
    end

    subgraph GHA["GitHub Actions"]
        WT["train.yml\ncada domingo 3am UTC"]
        WK["keepalive.yml\ncada 3 días"]
        WC["collect.yml\nbackup · cada 15 min"]
    end

    subgraph SRC["src/"]
        MAIN["main.py"]
        TRN["train.py"]
        PRD["predict.py"]
    end

    PKL["ecobici_model.pkl\nGitHub Release"]
    DASH["Dashboard\nStreamlit Cloud"]

    SCH -->|"HTTP POST"| EF
    EF -->|"GET"| API
    EF -->|"INSERT"| SN
    EF -->|"UPSERT"| SI

    WC -->|"ejecuta"| MAIN
    MAIN -->|"GET"| API
    MAIN -->|"INSERT"| SN
    MAIN -->|"UPSERT"| SI

    WT -->|"ejecuta"| TRN
    TRN -->|"SELECT"| SN
    TRN -->|"SELECT"| SI
    TRN -->|"guarda"| PKL

    WK -->|"SELECT COUNT(*)"| SN

    PKL -->|"carga"| PRD
    API -->|"GET estado actual"| PRD
    PRD -->|"imprime"| OUT["Reporte / Probabilidad"]

    SN -->|"lee"| DASH
    SI -->|"lee"| DASH
```

---

## Estructura del proyecto

```
ecobici-collector/
├── src/
│   ├── main.py            # Collector: dual trigger (GCP Scheduler + GitHub Actions)
│   ├── train.py           # Entrenamiento semanal del modelo
│   ├── predict.py         # CLI: predicción por estación o reporte completo
│   └── requirements.txt   # Dependencias del collector
├── app/
│   └── app.py             # Dashboard Streamlit con EDA y monitoreo
├── .github/
│   └── workflows/
│       ├── collect.yml    # Backup: cada 15 min vía GitHub Actions
│       ├── train.yml      # Entrenamiento: domingo 3am UTC
│       └── keepalive.yml  # Ping Supabase: cada 3 días
├── docs/
│   └── supabase_setup.sql # Schema completo: tablas, índices, vista
├── requirements.txt       # Dependencias generales (ML, dashboard)
├── requirements_app.txt   # Dependencias de Streamlit
├── .gitignore
└── README.md
```

---

## ¿Por qué Google Cloud en vez de GitHub Actions?

El proyecto inició con GitHub Actions como único scheduler. Funcionaba, pero presentó un problema estructural: **los crons de GitHub Actions son erráticos**.

### El problema

GitHub Actions ejecuta workflows con schedule (`cron`) en una cola compartida entre millones de repos. En la práctica esto significa:

- **Retrasos de 5 a 30 minutos** son normales, incluso en horarios no pico
- **Runs se saltan por completo** cuando la infraestructura de GitHub está saturada
- **Repos nuevos o con poca actividad se depriorizan** — el schedule puede tardar horas o incluso un día entero en activarse la primera vez
- **No hay garantía de intervalo**: un cron de `*/15` puede ejecutarse a los 8 min, luego a los 35, luego saltarse

Para un pipeline de recolección de datos que necesita consistencia temporal, esta variabilidad era inaceptable.

### La solución: Google Cloud Scheduler + Supabase

Google Cloud Scheduler es un servicio de cron gestionado con **precisión de segundos**. La arquitectura final es simple: Cloud Scheduler envía un HTTP POST cada 15 minutos a una función en Supabase que ejecuta la recolección.

Se intentó inicialmente usar Cloud Build + Cloud Run como runtime, pero la complejidad del pipeline de build (buildpacks, contenedores, service accounts) no justificaba el beneficio para una función que tarda ~5 segundos. La solución final es más limpia: el scheduler llama directamente a Supabase.

| Componente | Antes (GitHub Actions) | Ahora (Google Cloud + Supabase) |
|---|---|---|
| Scheduler | `cron: "*/15 * * * *"` en `.yml` | Google Cloud Scheduler (HTTP) |
| Runtime | Runner efímero Ubuntu | Supabase |
| Build pipeline | N/A | N/A (sin contenedores) |
| Precisión | ±5-30 min | ±1 segundo |
| Costo | Gratis (2,000 min/mes) | ~$0 (free tier de ambos) |

### Cómo funciona el dual trigger

El collector (`src/main.py`) detecta automáticamente quién lo invoca y registra el **origin** en la base de datos:

```python
def run_collector(request):
    ua = request.headers.get('User-Agent', '').lower()
    if 'google-cloud-scheduler' in ua:
        origin = 'google-cloud'
    elif 'github' in ua:
        origin = 'github-actions'
    else:
        origin = 'manual'
    return collect(origin)
```

Esto permite:
- **Google Cloud Scheduler** como fuente principal (preciso, confiable)
- **GitHub Actions** como backup (redundancia por si GCP falla)
- **Trazabilidad**: cada snapshot tiene registrado quién lo generó
- La restricción de unicidad (`collected_at, station_id`) evita duplicados si ambos corren al mismo momento

### ¿Por qué no se descartó GitHub Actions por completo?

GitHub Actions sigue siendo útil para:
- `train.yml` — el entrenamiento semanal no requiere precisión de segundos; una vez a la semana está bien
- `keepalive.yml` — el ping a Supabase cada 3 días es tolerante a retrasos
- `collect.yml` — funciona como red de seguridad en caso de fallo de GCP

---

## Scheduling y cron

### Google Cloud Scheduler — Recolección principal

```
Frecuencia: */15 * * * *  (cada 15 minutos)
Objetivo:   HTTP POST → Supabase (función de recolección)
Precisión:  < 1 segundo de desviación
```

El Cloud Scheduler envía un request HTTP que ejecuta la recolección. La función:

1. Detecta el origen del request (Google Cloud Scheduler)
2. Verifica si está dentro del horario operativo (05:00–00:30 CDMX)
3. Si está fuera de horario → responde 200 sin hacer nada
4. Si está dentro → descarga el API GBFS, persiste en Supabase, responde 200

### GitHub Actions — Entrenamiento y mantenimiento

#### `train.yml` — Entrenamiento semanal

```
cron: "0 3 * * 0"
         │ │   └── domingo (0)
         │ └────── 3am UTC = 9pm CDMX (sábado)
         └──────── minuto 0
```

Se reentrena cada domingo con todos los datos acumulados. El modelo resultante se publica como GitHub Release con el archivo `.pkl`.

#### `keepalive.yml` — Ping a Supabase

```
cron: "0 12 */3 * *"
         │  └── cada 3 días
         └───── 12pm UTC = 6am CDMX
```

Supabase pausa automáticamente los proyectos gratuitos que no reciben actividad por 7 días. Este workflow hace una consulta mínima (`SELECT COUNT(*)`) cada 3 días para mantener la base de datos activa.

#### `collect.yml` — Backup del collector

```
cron: "*/15 * * * *"  (cada 15 min, sujeto a variabilidad de GitHub)
```

Funciona como respaldo. Si Google Cloud Scheduler falla, GitHub Actions intenta la misma recolección. La restricción de unicidad en la DB evita duplicados.

---

## ¿Por qué Supabase?

Para este pipeline necesitábamos una base de datos PostgreSQL accesible desde la nube (tanto GCP como GitHub Actions), gratuita durante la fase de recolección, y que no requiriera administración de servidores. Supabase cumple todo eso.

### Alternativas consideradas

| Opción | Por qué se descartó |
|--------|---------------------|
| **SQLite en el repo** | GitHub Actions no garantiza persistencia entre runs; el repo crecería con datos binarios |
| **CSV en el repo** | Mismo problema de persistencia; git no es una base de datos |
| **Google Sheets** | API lenta, sin soporte real para SQL, difícil de escalar |
| **PlanetScale / Turso** | No son PostgreSQL nativo; requieren adaptadores extra |
| **Railway / Render DB** | Plan gratuito muy limitado en horas de cómputo |
| **GCP Cloud SQL** | Excelente, pero tiene costo desde el día 1 (~$7/mes mínimo) |

### Ventajas concretas de Supabase en este proyecto

- **PostgreSQL completo**: SQL arbitrario, `ON CONFLICT`, índices, vistas y triggers.
- **Acceso directo por connection string**: `psycopg2` se conecta igual que a cualquier Postgres. Sin SDK propietario.
- **Plan gratuito generoso**: 500 MB de almacenamiento, suficiente para años de snapshots.
- **Sin servidor que mantener**: se paga con tiempo de setup, no con dinero ni con ops.
- **Accesible desde cualquier nube**: tanto GCP como GitHub Actions se conectan con la misma connection string.

---

## ¿Por qué 15 minutos?

El intervalo de recolección es un balance entre **granularidad estadística** y **costo operativo**.

| Intervalo | Runs/mes | Filas/mes (677 estaciones) | Notas |
|-----------|----------|----------------------------|-------|
| 5 min | 8,640 | ~5.8 M | Excesivo para patrones horarios |
| 10 min | 4,320 | ~2.9 M | Viable pero sin beneficio real vs 15 |
| **15 min** | **2,880** | **~1.9 M** | **Balance óptimo** |
| 30 min | 1,440 | ~975 K | Pierde granularidad en hora punta |

Los patrones de disponibilidad de bicicletas son fenómenos de escala horaria (hora punta mañana, mediodía, tarde), no de escala de segundos. Una granularidad de 15 minutos captura con fidelidad suficiente la variación intradiaria que el modelo necesita aprender.

---

## Costos y límites gratuitos

El proyecto está diseñado para correr **completamente gratis** o con un costo mínimo.

### Google Cloud Platform

| Servicio | Uso mensual | Free tier | Costo real |
|----------|-------------|-----------|------------|
| Cloud Scheduler | ~2,880 invocaciones | 3 jobs gratis | $0 |
| **Total GCP** | | | **$0** |

### GitHub Actions — 2,000 min/mes (se renuevan el 1° de cada mes)

| Workflow | Runs/mes | Min/run | Min/mes |
|----------|----------|---------|---------|
| `collect.yml` backup (en horario operativo) | ~2,340 | ~0.5 | ~1,170 |
| `collect.yml` fuera de horario (salida rápida) | ~540 | ~0.05 | ~27 |
| `train.yml` semanal | ~4 | ~5 | ~20 |
| `keepalive.yml` cada 3 días | ~10 | ~0.3 | ~3 |
| **Total** | | | **~1,220 de 2,000** |

> Si en algún mes se agotan los minutos, los workflows se pausan hasta el día 1 del siguiente mes. No hay cargo adicional. La recolección principal (GCP) no se ve afectada.

### Supabase — 500 MB incluidos

| Concepto | Valor |
|----------|-------|
| Estaciones activas | ~677 |
| Snapshots por día (horario operativo) | ~2,600 |
| Crecimiento mensual | ~8 MB/mes |
| **Meses antes de llegar al límite** | **~60** |

Cuando quieras liberar espacio:

```sql
DELETE FROM snapshots WHERE collected_at < NOW() - INTERVAL '90 days';
```

### Streamlit Community Cloud — gratuito

El dashboard corre en los servidores de Streamlit y se redespliegua automáticamente con cada push a `main`. Sin costo.

---

## El modelo

El objetivo es clasificar cada combinación (estación, hora, día de semana) como `disponible` (≥ 1 bici) o `no disponible`.

### Features

| Feature | Descripción |
|---------|-------------|
| `station_enc` | ID de estación codificado numéricamente |
| `hour_sin`, `hour_cos` | Codificación cíclica de la hora (0–23) |
| `dow_sin`, `dow_cos` | Codificación cíclica del día de semana (0–6) |
| `is_weekend` | 1 si sábado o domingo |
| `capacity` | Capacidad total de la estación (si está disponible) |

La codificación cíclica con seno/coseno es importante: le dice al modelo que las 23:45 y las 00:15 son momentos cercanos, algo que una codificación lineal no capturaría.

### Algoritmo y parámetros

| Parámetro | Valor | Razón |
|-----------|-------|-------|
| Algoritmo | `GradientBoostingClassifier` | Robusto con features mixtas, no requiere normalización |
| `n_estimators` | 300 | Suficiente capacidad sin sobreajuste con los datos disponibles |
| `max_depth` | 4 | Árboles moderadamente profundos, evita memorización |
| `learning_rate` | 0.05 | Conservador; mejor generalización |
| Métrica | AUC-ROC | Adecuada para clasificación binaria con posible desbalance de clases |

### Horario operativo

El entrenamiento **filtra automáticamente** los snapshots fuera del horario operativo de EcoBici (00:30–05:00 CDMX). Esto evita que el modelo aprenda que "no hay bicis" cuando en realidad el sistema estaba cerrado.

### Reentrenamiento

El modelo se reentrena **cada domingo a las 3am UTC** vía GitHub Actions. Cada versión se publica como GitHub Release con el archivo `.pkl`.

---

## Setup paso a paso

### 1. Crear tablas en Supabase

1. Crea un proyecto en [supabase.com](https://supabase.com)
2. Ve a **SQL Editor** en el menú lateral
3. Copia y ejecuta el contenido de [`docs/supabase_setup.sql`](docs/supabase_setup.sql)

Esto crea las tablas `station_info` y `snapshots` (con columnas `bikes_disabled`, `docks_disabled`, `origin`), índices y la vista `snapshots_view`.

### 2. Obtener la connection string

En tu proyecto de Supabase: **Connect → Connection String → Transaction pooler**

```
postgresql://postgres.[ref]:[password]@aws-0-us-east-1.pooler.supabase.com:6543/postgres
```

> Usar **Transaction pooler** (puerto 6543), no la conexión directa (5432), para compatibilidad con servicios externos (GCP, GitHub Actions, Streamlit).

### 3. Configurar Google Cloud Scheduler

```bash
# Autenticarse
gcloud auth login
gcloud config set project TU_PROJECT_ID

# Crear el job del scheduler
gcloud scheduler jobs create http ecobici-collect \
  --schedule="*/15 * * * *" \
  --uri="URL_DE_TU_FUNCION_SUPABASE" \
  --http-method=POST \
  --time-zone="America/Mexico_City"
```

El scheduler enviará un HTTP POST cada 15 minutos. La función de recolección se encarga del resto.

### 4. Agregar el secret en GitHub

En tu repositorio: `Settings → Secrets and variables → Actions → New repository secret`

| Nombre | Valor |
|--------|-------|
| `SUPABASE_DB_URL` | La connection string del paso 2 |

### 5. Verificar

```sql
-- En Supabase SQL Editor
SELECT origin, COUNT(*), MIN(collected_at), MAX(collected_at)
FROM snapshots
GROUP BY origin;
```

Deberías ver filas con `origin = 'google-cloud'` y/o `origin = 'github-actions'`.

---

## Uso del CLI de predicción

Requiere tener `ecobici_model.pkl` descargado desde los GitHub Releases y `SUPABASE_DB_URL` en el entorno.

```bash
# Instalar dependencias
pip install -r requirements.txt

# Predicción para una estación específica
python src/predict.py --station 059 --hour 8 --dow 0   # Lunes 8am, estación 059

# Reporte en tiempo real de todas las estaciones
python src/predict.py --report
```

**Días de la semana:** `0` = Lunes … `6` = Domingo

**Semáforo de probabilidad:**

| Indicador | Rango | Significado |
|-----------|-------|-------------|
| Alta probabilidad | >= 70% | Es muy probable encontrar bici |
| Probabilidad moderada | 40–70% | Puede haber bici, no es seguro |
| Baja probabilidad | < 40% | Poco probable encontrar bici |

---

## Dashboard

El proyecto incluye un dashboard en Streamlit desplegado en [Streamlit Community Cloud](https://share.streamlit.io).

**Secciones:**
- Métricas principales: registros totales, estaciones, disponibilidad media, % bicis descompuestas
- Estado del pipeline: últimas 6 ejecuciones de `collect.yml` con status en tiempo real
- EDA de disponibilidad: histograma, barras por hora, heatmap hora x día, top/bottom estaciones
- Análisis de bicis descompuestas: timeline, ranking por estación

**Despliegue:**
1. Ve a [share.streamlit.io](https://share.streamlit.io)
2. Selecciona el repo `jmtoral/ecobici-collector`
3. Main file: `app/app.py`
4. En Secrets agrega: `SUPABASE_DB_URL = "postgresql://..."`

---

## Dependencias

### Collector (`src/requirements.txt`)

| Paquete | Uso |
|---------|-----|
| `requests` | Llamadas al API GBFS |
| `psycopg2-binary` | Conexión a Supabase/PostgreSQL |
| `functions-framework` | Runtime HTTP para invocaciones vía scheduler |

### Entrenamiento y predicción (`requirements.txt`)

| Paquete | Uso |
|---------|-----|
| `pandas` | Manipulación de datos tabulares |
| `numpy` | Transformaciones matemáticas (seno/coseno cíclico) |
| `scikit-learn` | Modelo de clasificación y métricas |
| `psycopg2-binary` | Conexión a Supabase |
| `requests` | API GBFS |

### Dashboard (`requirements_app.txt`)

| Paquete | Uso |
|---------|-----|
| `streamlit` | Framework del dashboard |
| `plotly` | Gráficas interactivas |
| `psycopg2-binary` | Conexión a Supabase |
| `pandas` | Datos tabulares |
| `requests` | GitHub API (estado del pipeline) |

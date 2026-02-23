# EcoBici Availability Predictor

Pipeline automatizado para recolectar datos de disponibilidad de bicicletas en **EcoBici (CDMX)** y entrenar un modelo de clasificación que estima la **probabilidad de encontrar al menos una bicicleta funcional** en una estación dada, a cualquier hora del día y día de la semana.

---

## Tabla de contenidos

1. [¿Qué hace este proyecto?](#qué-hace-este-proyecto)
2. [Arquitectura](#arquitectura)
3. [Estructura del proyecto](#estructura-del-proyecto)
4. [¿Por qué Supabase?](#por-qué-supabase)
5. [¿Por qué 15 minutos?](#por-qué-15-minutos)
6. [El modelo](#el-modelo)
7. [Setup paso a paso](#setup-paso-a-paso)
8. [Uso del CLI de predicción](#uso-del-cli-de-predicción)
9. [Dependencias](#dependencias)

---

## ¿Qué hace este proyecto?

EcoBici expone el estado de sus estaciones en tiempo real a través de un API público en formato [GBFS](https://gbfs.org/) (General Bikeshare Feed Specification). Cada llamada devuelve, para cada estación, cuántas bicis y cuántos espacios libres hay en ese momento.

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

    subgraph GHA["GitHub Actions"]
        WC["collect.yml\ncada 15 min"]
        WT["train.yml\ncada domingo 3am UTC"]
        WK["keepalive.yml\ncada 3 días"]
    end

    subgraph SRC["src/"]
        COL["collector.py"]
        TRN["train.py"]
        PRD["predict.py"]
    end

    subgraph DB["Supabase (PostgreSQL)"]
        SI["station_info\nnombre, capacidad, lat/lon"]
        SN["snapshots\nbikes_available por estación y timestamp"]
    end

    PKL["ecobici_model.pkl\nGitHub Release"]

    WC -->|"ejecuta"| COL
    COL -->|"GET"| API
    COL -->|"INSERT"| SN
    COL -->|"UPSERT"| SI

    WT -->|"ejecuta"| TRN
    TRN -->|"SELECT"| SN
    TRN -->|"SELECT"| SI
    TRN -->|"guarda"| PKL

    WK -->|"SELECT COUNT(*)"| SN

    PKL -->|"carga"| PRD
    API -->|"GET estado actual"| PRD
    PRD -->|"imprime"| OUT["Reporte / Probabilidad"]
```

---

## Estructura del proyecto

```
ecobici-collector/
├── src/
│   ├── collector.py    # Descarga el API GBFS y persiste en Supabase
│   ├── train.py        # Entrena GradientBoostingClassifier con los snapshots
│   └── predict.py      # CLI: predicción por estación o reporte completo
├── .github/
│   └── workflows/
│       ├── collect.yml    # Trigger: cada 15 min
│       ├── train.yml      # Trigger: cada domingo a las 3am UTC
│       └── keepalive.yml  # Trigger: cada 3 días (evita pausa en Supabase free)
├── docs/
│   └── supabase_setup.sql  # Schema completo: tablas, índices, vista
├── requirements.txt
├── .gitignore
└── README.md
```

---

## ¿Por qué Supabase?

Para este pipeline necesitábamos una base de datos PostgreSQL accesible desde GitHub Actions (nube → nube), gratuita durante la fase de recolección, y que no requiriera administración de servidores. Supabase cumple todo eso.

### Alternativas consideradas

| Opción | Por qué se descartó |
|--------|---------------------|
| **SQLite en el repo** | GitHub Actions no garantiza persistencia entre runs; el repo crecería con datos binarios |
| **CSV en el repo** | Mismo problema de persistencia; git no es una base de datos |
| **Google Sheets** | API lenta, sin soporte real para SQL, difícil de escalar |
| **PlanetScale / Turso** | No son PostgreSQL nativo; requieren adaptadores extra |
| **Railway / Render DB** | Plan gratuito muy limitado en horas de cómputo |
| **AWS RDS / GCP Cloud SQL** | Excelentes, pero tienen costo desde el día 1 |

### Ventajas concretas de Supabase en este proyecto

- **PostgreSQL completo**: podemos escribir SQL arbitrario, usar `ON CONFLICT`, índices, vistas y triggers — exactamente lo que usa el schema.
- **Acceso directo por connection string**: `psycopg2` se conecta igual que a cualquier Postgres. Sin SDK propietario.
- **Plan gratuito generoso**: 500 MB de almacenamiento, suficiente para meses de snapshots (~1 KB por fila × 280 estaciones × 96 snapshots/día ≈ **27 MB/mes**).
- **Sin servidor que mantener**: se paga con tiempo de setup, no con dinero ni con ops.
- **Pausa automática mitigada**: el workflow `keepalive.yml` hace un ping cada 3 días para evitar que Supabase pause el proyecto por inactividad (comportamiento del free tier).

### Cuándo Supabase dejaría de ser suficiente

Si el proyecto escala a recolección por segundo, o si se necesita más de 500 MB de datos históricos, la migración natural sería a una instancia de PostgreSQL dedicada (Railway Pro, Neon, o RDS). El código no cambiaría — solo la connection string.

---

## ¿Por qué 15 minutos?

El intervalo de recolección es un balance entre **granularidad estadística** y **costo operativo**.

| Intervalo | Runs/mes | Minutos Actions/mes | Filas/mes (280 estaciones) | ¿Viable en free? |
|-----------|----------|---------------------|-----------------------------|------------------|
| 5 min | 8,640 | ~8,640 | ~2.4 M | No (límite: 2,000 min) |
| 10 min | 4,320 | ~4,320 | ~1.2 M | Ajustado |
| **15 min** | **2,880** | **~2,880** | **~807 K** | **Sí** |
| 30 min | 1,440 | ~1,440 | ~403 K | Sí, pero pierde granularidad hora punta |

Además, los patrones de disponibilidad de bicicletas son fenómenos de escala horaria (hora punta mañana, mediodía, tarde), no de escala de segundos. Una granularidad de 15 minutos captura con fidelidad suficiente la variación intradiaria que el modelo necesita aprender.

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

### Reentrenamiento

El modelo se reentrena **cada domingo a las 3am UTC** usando todos los snapshots acumulados. Cada versión se publica como un GitHub Release con el archivo `.pkl`, lo que permite comparar el desempeño semana a semana.

---

## Setup paso a paso

### 1. Crear tablas en Supabase

1. Crea un proyecto en [supabase.com](https://supabase.com)
2. Ve a **SQL Editor** en el menú lateral
3. Copia y ejecuta el contenido de [`docs/supabase_setup.sql`](docs/supabase_setup.sql)

Esto crea las tablas `station_info` y `snapshots`, sus índices y una vista `snapshots_view` para exploración.

### 2. Obtener la connection string

En tu proyecto de Supabase: `Project Settings → Database → Connection string`

Selecciona el modo **Session** y copia la URL. Tendrá este formato:

```
postgresql://postgres.[ref]:[password]@aws-0-us-east-1.pooler.supabase.com:5432/postgres
```

### 3. Agregar el secret en GitHub

En tu repositorio: `Settings → Secrets and variables → Actions → New repository secret`

| Nombre | Valor |
|--------|-------|
| `SUPABASE_DB_URL` | La connection string del paso anterior |

### 4. Activar workflows

Los workflows se activan automáticamente al hacer push a `main`. Para probar de inmediato:

1. Ve a la pestaña **Actions** de tu repo
2. Selecciona **"Recolectar Snapshots EcoBici"**
3. Haz clic en **"Run workflow"**
4. Verifica en Supabase que hay datos en `station_info` y `snapshots`

---

## Uso del CLI de predicción

Requiere tener `ecobici_model.pkl` descargado desde los GitHub Releases, y `SUPABASE_DB_URL` en el entorno.

```bash
# Instalar dependencias
pip install -r requirements.txt

# Predicción para una estación específica
# Estación 059, Lunes (0) a las 8am
python src/predict.py --station 059 --hour 8 --dow 0

# Reporte en tiempo real de todas las estaciones (usa el API + el modelo)
python src/predict.py --report
```

**Días de la semana:** `0` = Lunes … `6` = Domingo

**Semáforo de probabilidad:**

| Indicador | Rango | Significado |
|-----------|-------|-------------|
| Alta probabilidad | ≥ 70% | Es muy probable encontrar bici |
| Probabilidad moderada | 40–70% | Puede haber bici, no es seguro |
| Baja probabilidad | < 40% | Poco probable encontrar bici |

---

## Dependencias

Ver [`requirements.txt`](requirements.txt).

| Paquete | Uso |
|---------|-----|
| `psycopg2-binary` | Conexión a Supabase/PostgreSQL |
| `requests` | Llamadas al API GBFS de EcoBici |
| `pandas` | Manipulación de datos tabulares |
| `numpy` | Transformaciones matemáticas (seno/coseno cíclico) |
| `scikit-learn` | Modelo de clasificación y métricas |

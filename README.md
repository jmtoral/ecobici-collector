# EcoBici Availability Predictor

Pipeline automatizado para recolectar datos de disponibilidad de bicicletas en EcoBici (CDMX) y entrenar un modelo de clasificación que estima la **probabilidad de encontrar al menos una bicicleta funcional** en una estación dada.

## Arquitectura

```
GitHub Actions (cada 15 min)
        │
        ▼
GBFS API (EcoBici)  ──►  src/collector.py  ──►  Supabase (PostgreSQL)
                                                         │
                                              (cada domingo 3am UTC)
                                                         │
                                                         ▼
                                               src/train.py  ──►  ecobici_model.pkl
                                                                         │
                                                                         ▼
                                                               src/predict.py (CLI)
```

## Estructura del proyecto

```
ecobici-collector/
├── src/
│   ├── collector.py    # Recolecta snapshots del API GBFS → Supabase
│   ├── train.py        # Entrena GradientBoostingClassifier desde Supabase
│   └── predict.py      # CLI de predicción e informes en tiempo real
├── .github/
│   └── workflows/
│       ├── collect.yml    # Ejecución cada 15 min
│       ├── train.yml      # Reentrenamiento semanal (domingo 3am UTC)
│       └── keepalive.yml  # Ping a Supabase cada 3 días
├── docs/
│   └── supabase_setup.sql  # Schema de tablas en Supabase
├── requirements.txt
└── .gitignore
```

## Setup

### 1. Supabase

Crea un proyecto en [supabase.com](https://supabase.com) y ejecuta el schema:

```sql
-- docs/supabase_setup.sql
```

Copia la **Connection string** (modo `Session` o `Transaction`) desde:
`Project Settings → Database → Connection string`

### 2. GitHub Secret

En tu repositorio: `Settings → Secrets and variables → Actions → New repository secret`

| Nombre | Valor |
|--------|-------|
| `SUPABASE_DB_URL` | `postgresql://postgres:[password]@[host]:5432/postgres` |

### 3. Activar workflows

Los tres workflows se activan automáticamente al hacer push. También puedes correrlos manualmente desde la pestaña **Actions** de GitHub.

## Uso del CLI de predicción

Requiere tener `ecobici_model.pkl` en la raíz del proyecto y `SUPABASE_DB_URL` en el entorno.

```bash
# Probabilidad para una estación específica
python src/predict.py --station 059 --hour 8 --dow 0   # Lunes 8am, estación 059

# Reporte en tiempo real de todas las estaciones
python src/predict.py --report
```

**Días de la semana:** 0=Lunes … 6=Domingo

**Semáforo de probabilidad:**
- `>= 70%` — alta probabilidad
- `40–70%` — probabilidad moderada
- `< 40%` — baja probabilidad

## Modelo

| Parámetro | Valor |
|-----------|-------|
| Algoritmo | `GradientBoostingClassifier` (scikit-learn) |
| Target | Binario: `bikes_available >= 1` |
| Features | `station_enc`, `hour_sin/cos`, `dow_sin/cos`, `is_weekend`, `capacity` |
| Métrica | AUC-ROC |
| Reentrenamiento | Cada domingo, incorporando todos los datos acumulados |

Los modelos se publican como GitHub Releases con el archivo `.pkl`.

## Sobre el intervalo de recolección

Se eligieron **15 minutos** por las siguientes razones:

- Los patrones de uso de bicicletas responden a ciclos horarios y diarios, no a cambios de minuto a minuto
- 5 min genera ~8,640 ejecuciones/mes (excede el plan gratuito de GitHub Actions de 2,000 min/mes)
- 15 min genera ~2,880 ejecuciones/mes, dentro del límite gratuito
- Supabase free tier tiene límite de almacenamiento; 15 min reduce el volumen de datos en 3×
- La granularidad de 15 min es suficiente para capturar la variación intradiaria relevante

## Dependencias

Ver [requirements.txt](requirements.txt). Las principales:

- `psycopg2-binary` — conexión a Supabase/PostgreSQL
- `requests` — llamadas al API GBFS
- `pandas` + `numpy` — manipulación de datos
- `scikit-learn` — modelo de clasificación

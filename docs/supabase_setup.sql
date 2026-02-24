-- =============================================================================
-- EcoBici Collector — Schema de Supabase
-- Ejecutar una vez en el SQL Editor de tu proyecto Supabase
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Tabla: station_info
-- Información estática de cada estación (actualizada en cada recolección).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS station_info (
    station_id  TEXT        PRIMARY KEY,
    name        TEXT,
    capacity    INTEGER,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Actualizar updated_at automáticamente
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER station_info_updated_at
    BEFORE UPDATE ON station_info
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- -----------------------------------------------------------------------------
-- Tabla: snapshots
-- Un registro por estación por recolección (cada 15 minutos).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS snapshots (
    id               BIGSERIAL    PRIMARY KEY,
    collected_at     TIMESTAMPTZ  NOT NULL,
    station_id       TEXT         NOT NULL REFERENCES station_info(station_id),
    bikes_available  INTEGER      NOT NULL DEFAULT 0,
    bikes_disabled   INTEGER      NOT NULL DEFAULT 0,
    docks_available  INTEGER      NOT NULL DEFAULT 0,
    docks_disabled   INTEGER      NOT NULL DEFAULT 0,
    is_installed     BOOLEAN      NOT NULL DEFAULT TRUE,
    is_renting       BOOLEAN      NOT NULL DEFAULT FALSE,
    is_returning     BOOLEAN      NOT NULL DEFAULT FALSE
);

-- Índice principal para consultas de entrenamiento (por tiempo y estación)
CREATE INDEX IF NOT EXISTS idx_snapshots_collected_at
    ON snapshots (collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_snapshots_station_collected
    ON snapshots (station_id, collected_at DESC);

-- Restricción de unicidad: evita duplicados si el workflow se ejecuta dos veces
-- con el mismo feed timestamp
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_unique
    ON snapshots (collected_at, station_id);

-- -----------------------------------------------------------------------------
-- Vista: snapshots_view
-- Agrega información de la estación para consultas de análisis/debugging.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW snapshots_view AS
SELECT
    s.id,
    s.collected_at,
    s.station_id,
    si.name              AS station_name,
    s.bikes_available,
    s.bikes_disabled,
    s.docks_available,
    s.docks_disabled,
    si.capacity,
    (s.bikes_available + s.bikes_disabled) AS bikes_total,
    CASE WHEN s.bikes_available >= 1 THEN TRUE ELSE FALSE END AS disponible,
    s.is_installed,
    s.is_renting,
    s.is_returning
FROM snapshots s
LEFT JOIN station_info si USING (station_id);

-- -----------------------------------------------------------------------------
-- Notas de mantenimiento
-- -----------------------------------------------------------------------------
-- Para ver el conteo de snapshots por día:
--   SELECT DATE(collected_at) AS dia, COUNT(*) FROM snapshots GROUP BY 1 ORDER BY 1 DESC;
--
-- Para ver estaciones con más datos:
--   SELECT station_id, COUNT(*) AS n FROM snapshots GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
--
-- Para eliminar datos muy antiguos (opcional, si se acerca al límite free):
--   DELETE FROM snapshots WHERE collected_at < NOW() - INTERVAL '90 days';

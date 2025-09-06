BEGIN;

CREATE SCHEMA IF NOT EXISTS banorte_conceptos;
DROP TABLE IF EXISTS banorte_conceptos.credito_corriente;

CREATE TABLE banorte_conceptos.credito_corriente AS
WITH
-- Base table with normalized key
c AS (
  SELECT
    fecha,
    concepto,
    abono,
    cargo,
    tarjeta,
    file_date,
    file_name,
    -- keys (normalized)
    date(fecha)                          AS k_fecha,
    COALESCE(abono,0)::numeric(18,2)     AS k_abono,
    COALESCE(cargo,0)::numeric(18,2)     AS k_cargo
  FROM banorte_lake.credito_corriente
),

-- Enrichment table normalized and de-duplicated on the same key
cc_dedup AS (
  SELECT *
  FROM (
    SELECT
      fecha,
      concepto,
      abono,
      cargo,
      tarjeta,
      file_date,
      file_name,
      beneficiario,
      categoria,
      grupo,
      clave_presupuestal,
      concepto_procesado,
      -- keys (normalized)
      date(fecha)                          AS k_fecha,
      COALESCE(abono,0)::numeric(18,2)     AS k_abono,
      COALESCE(cargo,0)::numeric(18,2)     AS k_cargo,
      ROW_NUMBER() OVER (
        PARTITION BY date(fecha),
                     COALESCE(abono,0)::numeric(18,2),
                     COALESCE(cargo,0)::numeric(18,2)
        ORDER BY
          (CASE WHEN categoria     IS NOT NULL THEN 0 ELSE 1 END),
          (CASE WHEN beneficiario  IS NOT NULL THEN 0 ELSE 1 END),
          file_date DESC NULLS LAST,
          fecha     DESC NULLS LAST
      ) AS rn
    FROM banorte_lake.credito_con_conceptos
  ) t
  WHERE rn = 1
)

-- Build the mirror using ONLY (k_fecha, k_abono, k_cargo)
SELECT
  c.fecha,
  c.concepto,
  c.abono,
  c.cargo,
  c.tarjeta,
  c.file_date,
  c.file_name,
  cc.beneficiario,
  cc.categoria,
  cc.grupo,
  cc.clave_presupuestal,
  cc.concepto_procesado
FROM c
LEFT JOIN cc_dedup cc
  ON  c.k_fecha IS NOT DISTINCT FROM cc.k_fecha
  AND c.k_abono IS NOT DISTINCT FROM cc.k_abono
  AND c.k_cargo IS NOT DISTINCT FROM cc.k_cargo
;

-- Helpful indexes
CREATE INDEX ON banorte_conceptos.credito_corriente (file_date);
CREATE INDEX ON banorte_conceptos.credito_corriente (file_name);
CREATE INDEX ON banorte_conceptos.credito_corriente (categoria);

COMMIT;
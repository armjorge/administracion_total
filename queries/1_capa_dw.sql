CREATE SCHEMA IF NOT EXISTS dw_banorte;

-- Unificamos columnas (ajusta nombres si difieren)
-- Asumo esquema base (ajústalo a tus nombres reales):
-- fecha, concepto, abono, cargo, tarjeta, file_date, file_name
CREATE TABLE IF NOT EXISTS dw_banorte.movimientos (
  id BIGSERIAL PRIMARY KEY,
  tipo TEXT NOT NULL,     -- 'credito' | 'debito'
  estado TEXT NOT NULL,   -- 'corriente' | 'cerrado'
  fecha DATE NOT NULL,
  concepto TEXT,
  abono NUMERIC(14,2),
  cargo NUMERIC(14,2),
  tarjeta TEXT,
  file_date DATE,
  file_name TEXT
);

-- Carga inicial (replace lógica si necesitas)
INSERT INTO dw_banorte.movimientos (tipo, estado, fecha, concepto, abono, cargo, tarjeta, file_date, file_name)
SELECT 'credito' AS tipo, estado, fecha::date, concepto, abono::numeric, cargo::numeric, tarjeta, file_date::date, file_name
FROM stage_banorte.credito_all
UNION ALL
SELECT 'debito'  AS tipo, estado, fecha::date, concepto, abono::numeric, cargo::numeric, tarjeta, file_date::date, file_name
FROM stage_banorte.debito_all;

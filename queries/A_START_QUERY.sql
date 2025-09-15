-- Construye esquema banking_info con 4 tablas enriquecidas
-- Este scrip se debe ejecutar para actualizar la tabla de  _conceptos.                                               
BEGIN;

CREATE SCHEMA IF NOT EXISTS banking_info;

-- =========================================================
-- 1) CRÉDITO CERRADO + conceptos
-- =========================================================
DROP TABLE IF EXISTS banking_info.credito_cerrado;
CREATE TABLE banking_info.credito_cerrado (
    fecha DATE,
    unic_concept TEXT,
    cargo NUMERIC,
    abono NUMERIC,
    concepto TEXT,
    estado TEXT,
    beneficiario TEXT,
    categoria TEXT,
    grupo TEXT,
    id_presupuesto TEXT,
    concepto_procesado TEXT,
    ubicacion TEXT,
    file_date DATE,
    PRIMARY KEY (fecha, unic_concept, cargo, abono)
);

INSERT INTO banking_info.credito_cerrado (
    fecha, unic_concept, cargo, abono, concepto, estado,
    beneficiario, categoria, grupo, id_presupuesto, concepto_procesado, ubicacion, file_date
)
SELECT
    c.fecha, c.unic_concept, c.cargo, c.abono, c.concepto, c.estado,
    cc.beneficiario, cc.categoria, cc.grupo, cc.id_presupuesto, cc.concepto_procesado, cc.ubicacion,
    c.file_date
FROM banorte_load.credito_cerrado c
LEFT JOIN banorte_load.credito_conceptos cc
  ON c.fecha = cc.fecha
 AND c.unic_concept = cc.unic_concept
 AND c.cargo IS NOT DISTINCT FROM cc.cargo
 AND c.abono IS NOT DISTINCT FROM cc.abono;

-- =========================================================
-- 2) CRÉDITO CORRIENTE + conceptos
-- =========================================================
DROP TABLE IF EXISTS banking_info.credito_corriente;
CREATE TABLE banking_info.credito_corriente (
    fecha DATE,
    unic_concept TEXT,
    cargo NUMERIC,
    abono NUMERIC,
    concepto TEXT,
    estado TEXT,
    beneficiario TEXT,
    categoria TEXT,
    grupo TEXT,
    id_presupuesto TEXT,
    concepto_procesado TEXT,
    ubicacion TEXT,
    file_date DATE,
    PRIMARY KEY (fecha, unic_concept, cargo, abono)
);

INSERT INTO banking_info.credito_corriente (
    fecha, unic_concept, cargo, abono, concepto, estado,
    beneficiario, categoria, grupo, id_presupuesto, concepto_procesado, ubicacion, file_date
)
SELECT
    c.fecha, c.unic_concept, c.cargo, c.abono, c.concepto, c.estado,
    cc.beneficiario, cc.categoria, cc.grupo, cc.id_presupuesto, cc.concepto_procesado, cc.ubicacion,
    c.file_date
FROM banorte_load.credito_corriente c
LEFT JOIN banorte_load.credito_conceptos cc
  ON c.fecha = cc.fecha
 AND c.unic_concept = cc.unic_concept
 AND c.cargo IS NOT DISTINCT FROM cc.cargo
 AND c.abono IS NOT DISTINCT FROM cc.abono;

-- =========================================================
-- 3) DÉBITO CERRADO + conceptos
-- =========================================================
DROP TABLE IF EXISTS banking_info.debito_cerrado;
CREATE TABLE banking_info.debito_cerrado (
    fecha DATE,
    unic_concept TEXT,
    cargo NUMERIC,
    abono NUMERIC,
    concepto TEXT,
    estado TEXT,
    beneficiario TEXT,
    categoria TEXT,
    grupo TEXT,
    id_presupuesto TEXT,
    concepto_procesado TEXT,
    ubicacion TEXT,
    file_date DATE,
    PRIMARY KEY (fecha, unic_concept, cargo, abono)
);

INSERT INTO banking_info.debito_cerrado (
    fecha, unic_concept, cargo, abono, concepto, estado,
    beneficiario, categoria, grupo, id_presupuesto, concepto_procesado, ubicacion, file_date
)
SELECT
    d.fecha, d.unic_concept, d.cargo, d.abono, d.concepto, d.estado,
    dc.beneficiario, dc.categoria, dc.grupo, dc.id_presupuesto, dc.concepto_procesado, dc.ubicacion,
    d.file_date
FROM banorte_load.debito_cerrado d
LEFT JOIN banorte_load.debito_conceptos dc
  ON d.fecha = dc.fecha
 AND d.unic_concept = dc.unic_concept
 AND d.cargo IS NOT DISTINCT FROM dc.cargo
 AND d.abono IS NOT DISTINCT FROM dc.abono;

-- =========================================================
-- 4) DÉBITO CORRIENTE + conceptos
-- =========================================================
DROP TABLE IF EXISTS banking_info.debito_corriente;
CREATE TABLE banking_info.debito_corriente (
    fecha DATE,
    unic_concept TEXT,
    cargo NUMERIC,
    abono NUMERIC,
    concepto TEXT,
    estado TEXT,
    beneficiario TEXT,
    categoria TEXT,
    grupo TEXT,
    id_presupuesto TEXT,
    concepto_procesado TEXT,
    ubicacion TEXT,
    file_date DATE,
    PRIMARY KEY (fecha, unic_concept, cargo, abono)
);

INSERT INTO banking_info.debito_corriente (
    fecha, unic_concept, cargo, abono, concepto, estado,
    beneficiario, categoria, grupo, id_presupuesto, concepto_procesado, ubicacion, file_date
)
SELECT
    d.fecha, d.unic_concept, d.cargo, d.abono, d.concepto, d.estado,
    dc.beneficiario, dc.categoria, dc.grupo, dc.id_presupuesto, dc.concepto_procesado, dc.ubicacion,
    d.file_date
FROM banorte_load.debito_corriente d
LEFT JOIN banorte_load.debito_conceptos dc
  ON d.fecha = dc.fecha
 AND d.unic_concept = dc.unic_concept
 AND d.cargo IS NOT DISTINCT FROM dc.cargo
 AND d.abono IS NOT DISTINCT FROM dc.abono;

COMMIT;
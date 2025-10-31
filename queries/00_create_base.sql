-- Create schema 
CREATE SCHEMA IF NOT EXISTS banorte_load;

-- Create accounts table
CREATE TABLE IF NOT EXISTS banorte_load.accounts (
    account_number TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK (type IN ('credit', 'debit'))
);

-- Create debito, credito tables 

CREATE TABLE IF NOT EXISTS banorte_load.debito_cerrado (
    fecha DATE NOT NULL,
    concepto TEXT,
    cargo NUMERIC(12,2),
    abono NUMERIC(12,2),
    saldo NUMERIC(12,2),
    file_date DATE,
    file_name TEXT,
    estado TEXT CHECK (estado IN ('cerrado', 'abierto')),
    cuenta TEXT REFERENCES banorte_load.accounts(account_number),
    unique_concept TEXT,
    PRIMARY KEY (fecha, unique_concept, cargo, abono)
);

CREATE TABLE IF NOT EXISTS banorte_load.debito_abierto (
    fecha DATE NOT NULL,
    concepto TEXT,
    cargo NUMERIC(12,2),
    abono NUMERIC(12,2),
    saldo NUMERIC(12,2),
    file_date DATE,
    file_name TEXT,
    estado TEXT CHECK (estado IN ('cerrado', 'abierto')),
    cuenta TEXT REFERENCES banorte_load.accounts(account_number),
    unique_concept TEXT,
    PRIMARY KEY (fecha, unique_concept, cargo, abono)
);

CREATE TABLE IF NOT EXISTS banorte_load.credito_cerrado (
    fecha DATE NOT NULL,
    concepto TEXT,
    cargo NUMERIC(12,2),
    abono NUMERIC(12,2),
    saldo NUMERIC(12,2),
    file_date DATE,
    file_name TEXT,
    estado TEXT CHECK (estado IN ('cerrado', 'abierto')),
    cuenta TEXT REFERENCES banorte_load.accounts(account_number),
    unique_concept TEXT,
    PRIMARY KEY (fecha, unique_concept, cargo, abono)
);

CREATE TABLE IF NOT EXISTS banorte_load.credito_abierto (
    fecha DATE NOT NULL,
    concepto TEXT,
    cargo NUMERIC(12,2),
    abono NUMERIC(12,2),
    saldo NUMERIC(12,2),
    file_date DATE,
    file_name TEXT,
    estado TEXT CHECK (estado IN ('cerrado', 'abierto')),
    cuenta TEXT REFERENCES banorte_load.accounts(account_number),
    unique_concept TEXT,
    PRIMARY KEY (fecha, unique_concept, cargo, abono)
);

-----------------
---CUTOFF DAYS---
-----------------

-- Create cutoff days
CREATE TABLE IF NOT EXISTS banorte_load.cutoff_days (
    fecha DATE PRIMARY KEY,
    periodo TEXT NOT NULL
);

-- Function to populate cutoff days
CREATE OR REPLACE FUNCTION banorte_load.populate_cutoff_days(year_input INT DEFAULT EXTRACT(YEAR FROM CURRENT_DATE))
RETURNS VOID AS $$
DECLARE
    m INT;
    cutoff_date DATE;
    period TEXT;
BEGIN
    FOR m IN 1..12 LOOP
        cutoff_date := make_date(year_input, m, 6);
        period := to_char(cutoff_date, 'YYYY-MM');
        
        INSERT INTO banorte_load.cutoff_days (fecha, periodo)
        VALUES (cutoff_date, period)
        ON CONFLICT (fecha) DO NOTHING;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Dummy table to record years
CREATE TABLE IF NOT EXISTS banorte_load.cutoff_years (
    year_value INT PRIMARY KEY
);

-- Create cutoff days
CREATE TABLE IF NOT EXISTS banorte_load.cutoff_days (
    fecha DATE PRIMARY KEY,
    periodo TEXT NOT NULL
);

-- Function to populate cutoff days
CREATE OR REPLACE FUNCTION banorte_load.populate_cutoff_days(year_input INT DEFAULT EXTRACT(YEAR FROM CURRENT_DATE))
RETURNS VOID AS $$
DECLARE
    m INT;
    cutoff_date DATE;
    period TEXT;
BEGIN
    FOR m IN 1..12 LOOP
        cutoff_date := make_date(year_input, m, 6);
        period := to_char(cutoff_date, 'YYYY-MM');
        
        INSERT INTO banorte_load.cutoff_days (fecha, periodo)
        VALUES (cutoff_date, period)
        ON CONFLICT (fecha) DO NOTHING;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Table to record years needed for cutoff days generation
CREATE TABLE IF NOT EXISTS banorte_load.cutoff_years (
    year_value INT PRIMARY KEY
);

-- Functino to call populate_cutoff_days
CREATE OR REPLACE FUNCTION banorte_load.generate_cutoff_trigger()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM banorte_load.populate_cutoff_days(NEW.year_value);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger populate_cutoff_days
DROP TRIGGER IF EXISTS trg_generate_cutoff_days ON banorte_load.cutoff_years;

CREATE TRIGGER trg_generate_cutoff_days
AFTER INSERT ON banorte_load.cutoff_years
FOR EACH ROW
EXECUTE FUNCTION banorte_load.generate_cutoff_trigger();

-- ===========================================
-- Enriched Data Model with Sync & Audit Logic
-- ===========================================

-- 🔹 Tabla de categorías
CREATE TABLE IF NOT EXISTS banorte_load.category (
    id SERIAL PRIMARY KEY,
    "group" TEXT NOT NULL,
    subgroup TEXT NOT NULL,
    CONSTRAINT category_unique_pair UNIQUE ("group", subgroup)
);

-- 🔹 Tabla de beneficiarios
CREATE TABLE IF NOT EXISTS banorte_load.beneficiaries (
    id SERIAL PRIMARY KEY,
    nombre TEXT UNIQUE NOT NULL
);

-- ===========================================
-- Tablas enriquecidas
-- ===========================================

-- 🔹 Créditos
CREATE TABLE IF NOT EXISTS banorte_load.credito_conceptos (
    fecha DATE NOT NULL,
    unique_concept TEXT NOT NULL,
    cargo NUMERIC(14,2),
    abono NUMERIC(14,2),
    concepto TEXT,
    cuenta TEXT,
    estado TEXT,
    category_group TEXT,
    category_subgroup TEXT,
    beneficiario TEXT,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (fecha, unique_concept, cargo, abono),
    FOREIGN KEY (category_group, category_subgroup)
        REFERENCES banorte_load.category("group", subgroup)
        ON UPDATE CASCADE
        ON DELETE SET NULL,
    FOREIGN KEY (beneficiario)
        REFERENCES banorte_load.beneficiaries(nombre)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);

-- 🔹 Débitos
CREATE TABLE IF NOT EXISTS banorte_load.debito_conceptos (
    fecha DATE NOT NULL,
    unique_concept TEXT NOT NULL,
    cargo NUMERIC(14,2),
    abono NUMERIC(14,2),
    concepto TEXT,
    cuenta TEXT,
    estado TEXT,
    category_group TEXT,
    category_subgroup TEXT,
    beneficiario TEXT,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (fecha, unique_concept, cargo, abono),
    FOREIGN KEY (category_group, category_subgroup)
        REFERENCES banorte_load.category("group", subgroup)
        ON UPDATE CASCADE
        ON DELETE SET NULL,
    FOREIGN KEY (beneficiario)
        REFERENCES banorte_load.beneficiaries(nombre)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);

-- ===========================================
-- Función de sincronización 
-- ===========================================

CREATE OR REPLACE FUNCTION banorte_load.sync_conceptos()
RETURNS TRIGGER AS $$
DECLARE
    cuenta_tipo TEXT;
BEGIN
    -- Obtener tipo de cuenta desde la tabla accounts
    SELECT type INTO cuenta_tipo
    FROM banorte_load.accounts
    WHERE account_number = NEW.cuenta;

    -- Evitar procesamiento si no existe cuenta o tipo no definido
    IF cuenta_tipo IS NULL THEN
        RAISE NOTICE '⚠️ Cuenta % no encontrada en tabla accounts. Registro ignorado.', NEW.cuenta;
        RETURN NEW;
    END IF;

    -- Solo ejecutar si es una inserción o si el estado cambió
    IF (TG_OP = 'INSERT') OR (TG_OP = 'UPDATE' AND NEW.estado IS DISTINCT FROM OLD.estado) THEN

        -- Créditos → Solo si la cuenta es tipo "credit"
        IF TG_TABLE_NAME IN ('credito_abierto', 'credito_cerrado') AND cuenta_tipo = 'credit' THEN
            INSERT INTO banorte_load.credito_conceptos (
                fecha, unique_concept, cargo, abono, concepto, cuenta, estado, updated_at
            )
            VALUES (
                NEW.fecha, NEW.unique_concept, NEW.cargo, NEW.abono, NEW.concepto, NEW.cuenta, NEW.estado, NOW()
            )
            ON CONFLICT (fecha, unique_concept, cargo, abono)
            DO UPDATE SET 
                estado = EXCLUDED.estado,
                updated_at = NOW();

        -- Débitos → Solo si la cuenta es tipo "debit"
        ELSIF TG_TABLE_NAME IN ('debito_abierto', 'debito_cerrado') AND cuenta_tipo = 'debit' THEN
            INSERT INTO banorte_load.debito_conceptos (
                fecha, unique_concept, cargo, abono, concepto, cuenta, estado, updated_at
            )
            VALUES (
                NEW.fecha, NEW.unique_concept, NEW.cargo, NEW.abono, NEW.concepto, NEW.cuenta, NEW.estado, NOW()
            )
            ON CONFLICT (fecha, unique_concept, cargo, abono)
            DO UPDATE SET 
                estado = EXCLUDED.estado,
                updated_at = NOW();

        ELSE
            -- Si la cuenta no coincide con el tipo esperado, ignoramos
            RAISE NOTICE '⚠️ Registro ignorado: cuenta % pertenece a tipo %, pero proviene de tabla %',
                NEW.cuenta, cuenta_tipo, TG_TABLE_NAME;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ===========================================
-- Triggers de sincronización automática
-- ===========================================

-- Créditos
CREATE OR REPLACE TRIGGER trg_sync_credito_abierto
AFTER INSERT OR UPDATE ON banorte_load.credito_abierto
FOR EACH ROW EXECUTE FUNCTION banorte_load.sync_conceptos();

CREATE OR REPLACE TRIGGER trg_sync_credito_cerrado
AFTER INSERT OR UPDATE ON banorte_load.credito_cerrado
FOR EACH ROW EXECUTE FUNCTION banorte_load.sync_conceptos();

-- Débitos
CREATE OR REPLACE TRIGGER trg_sync_debito_abierto
AFTER INSERT OR UPDATE ON banorte_load.debito_abierto
FOR EACH ROW EXECUTE FUNCTION banorte_load.sync_conceptos();

CREATE OR REPLACE TRIGGER trg_sync_debito_cerrado
AFTER INSERT OR UPDATE ON banorte_load.debito_cerrado
FOR EACH ROW EXECUTE FUNCTION banorte_load.sync_conceptos();
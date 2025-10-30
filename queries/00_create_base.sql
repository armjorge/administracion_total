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
    unic_concept TEXT,
    PRIMARY KEY (fecha, unic_concept, cargo, abono)
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
    unic_concept TEXT,
    PRIMARY KEY (fecha, unic_concept, cargo, abono)
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
    unic_concept TEXT,
    PRIMARY KEY (fecha, unic_concept, cargo, abono)
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
    unic_concept TEXT,
    PRIMARY KEY (fecha, unic_concept, cargo, abono)
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

-- Dummy table to record years
CREATE TABLE IF NOT EXISTS banorte_load.cutoff_years (
    year_value INT PRIMARY KEY
);

-- Crear función del trigger (corregida)
CREATE OR REPLACE FUNCTION banorte_load.generate_cutoff_trigger()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM banorte_load.populate_cutoff_days(NEW.year_value);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Crear el trigger
DROP TRIGGER IF EXISTS trg_generate_cutoff_days ON banorte_load.cutoff_years;

CREATE TRIGGER trg_generate_cutoff_days
AFTER INSERT ON banorte_load.cutoff_years
FOR EACH ROW
EXECUTE FUNCTION banorte_load.generate_cutoff_trigger();


-- 0 Crear esquema
CREATE SCHEMA IF NOT EXISTS {schema};

-- 1. Tabla de años de corte
CREATE TABLE IF NOT EXISTS {schema}.cutoff_years (
    year_value INT PRIMARY KEY
);

-- 2. Tabla de días de corte -- Un día de corte por mes (por defecto el día 6), ligado a un año
CREATE TABLE IF NOT EXISTS {schema}.cutoff_days (
    year_value   INT NOT NULL REFERENCES {schema}.cutoff_years(year_value) ON DELETE CASCADE,
    cutoff_date  DATE NOT NULL,   
    period       TEXT NOT NULL,  
    PRIMARY KEY (year_value, period),
    UNIQUE (cutoff_date),
    CHECK (EXTRACT(YEAR FROM cutoff_date) = year_value)
);



-- Función del trigger: insertar día 6 de cada mes del año dado
CREATE OR REPLACE FUNCTION {schema}.populate_cutoff_days_for_year()
RETURNS TRIGGER AS $$
DECLARE
    m INT;
    cutoff_dt DATE;
    period_txt TEXT;
BEGIN
    FOR m IN 1..12 LOOP
        cutoff_dt := make_date(NEW.year_value, m, 6);
        period_txt := format('%s-%02s', NEW.year_value, m);

        INSERT INTO {schema}.cutoff_days (year_value, cutoff_date, period)
        VALUES (NEW.year_value, cutoff_dt, period_txt)
        ON CONFLICT (year_value, period) DO NOTHING;
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger: después de insertar un año, se generan sus 12 cortes
DROP TRIGGER IF EXISTS trg_populate_cutoff_days ON {schema}.cutoff_years;

CREATE TRIGGER trg_populate_cutoff_days
AFTER INSERT ON {schema}.cutoff_years
FOR EACH ROW
EXECUTE FUNCTION {schema}.populate_cutoff_days_for_year();

-- Tabla de cuentas
CREATE TABLE IF NOT EXISTS {schema}.accounts (
    account_number TEXT NOT NULL,
    type           TEXT NOT NULL CHECK (type IN ('credit', 'debit')),
    PRIMARY KEY (account_number, type)
);

-- Tabla de periodos de corte por cuenta
CREATE TABLE IF NOT EXISTS {schema}.account_cutoffs (
    account_number TEXT NOT NULL,
    type           TEXT NOT NULL CHECK (type IN ('credit', 'debit')),
    cutoff_period  TEXT NOT NULL,  
    updated_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (account_number, cutoff_period),
    FOREIGN KEY (account_number, type)
        REFERENCES {schema}.accounts(account_number, type)
);


CREATE OR REPLACE FUNCTION {schema}.refresh_account_cutoffs()
RETURNS VOID AS $$
BEGIN
    INSERT INTO {schema}.account_cutoffs (account_number, type, cutoff_period, updated_at)
    SELECT 
        a.account_number,
        a.type,
        d.period AS cutoff_period,
        NOW()
    FROM 
        {schema}.accounts a
    JOIN 
        {schema}.cutoff_days d
        ON (

            (a.type = 'credit' AND d.cutoff_date < CURRENT_DATE)
            OR

            (a.type = 'debit'  AND d.cutoff_date < CURRENT_DATE)
        )
    ON CONFLICT (account_number, cutoff_period)
    DO UPDATE SET
        updated_at = EXCLUDED.updated_at; 
END;
$$ LANGUAGE plpgsql;


-- 3️⃣ Habilitar pg_cron (si no está instalado)
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 4️⃣ Programar job diario a las 00:05
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM cron.job WHERE jobname = 'update_account_cutoffs_daily'
    ) THEN
        PERFORM cron.schedule(
            'update_account_cutoffs_daily',
            '5 0 * * *',  -- todos los días a las 00:05
            'SELECT {schema}.refresh_account_cutoffs();'
        );
    END IF;
END;
$$;


-- 5️⃣ Poblar de inicio los cortes por cuenta
SELECT {schema}.refresh_account_cutoffs();
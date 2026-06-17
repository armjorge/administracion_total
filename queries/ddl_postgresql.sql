CREATE TABLE dim_year (
    year_id SERIAL PRIMARY KEY,
    year INT UNIQUE NOT NULL CHECK (year BETWEEN 1000 AND 9999)
);


CREATE TABLE dim_acc_type (
    type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(10) UNIQUE NOT NULL CHECK (type_name IN ('credit', 'debit'))
);

INSERT INTO dim_acc_type (type_name)
VALUES 
('credit'),
('debit');


CREATE TABLE dim_accounts (
    account_id SERIAL PRIMARY KEY,
    account VARCHAR(4) UNIQUE NOT NULL CHECK (account ~ '^[0-9]{4}$'),
    type_id INT NOT NULL,
    start_date timestamptz DEFAULT DATE_TRUNC('month', CURRENT_TIMESTAMP) NOT NULL,
    cutoff_day INT, 
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT dim_company_ctype_id_fkey FOREIGN KEY (type_id) REFERENCES dim_acc_type(type_id)
);

CREATE TABLE dim_period (
    period_id SERIAL PRIMARY KEY,
    account_id INT NOT NULL,
    period VARCHAR(7) NOT NULL, -- Format: '2026-06'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (account_id, period) -- Prevents duplicate account/period rows
);


CREATE OR REPLACE FUNCTION refresh_account_periods()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO dim_period (account_id, period)
    SELECT 
        a.account_id,
        TO_CHAR(m.month_date, 'YYYY-MM') AS period
    FROM 
        dim_accounts a
    INNER JOIN 
        dim_acc_type t ON a.type_id = t.type_id
    -- Generate a row for the 1st of every month between start_date and today
    CROSS JOIN LATERAL 
        generate_series(
            DATE_TRUNC('month', a.start_date), 
            DATE_TRUNC('month', CURRENT_TIMESTAMP), 
            INTERVAL '1 month'
        ) AS m(month_date)
    -- We define a dynamic 'calculated_cutoff' timestamp inside a LATERAL subquery 
    -- so we don't have to repeat the complex CASE statement twice in the WHERE clause.
    CROSS JOIN LATERAL (
        SELECT CASE 
            -- If cutoff_day is provided, use it
            WHEN a.cutoff_day IS NOT NULL 
                THEN m.month_date + (a.cutoff_day - 1) * INTERVAL '1 day'
            -- If cutoff_day is NULL, make it the last day of that month
            ELSE (m.month_date + INTERVAL '1 month') - INTERVAL '1 day'
        END AS calculated_cutoff
    ) c
    WHERE 
        -- Rule 1: Current date must be past the calculated cutoff date
        CURRENT_TIMESTAMP >= c.calculated_cutoff
        
        -- Rule 2: Ensure the calculated cutoff date is also after or equal to the actual start_date
        AND c.calculated_cutoff >= a.start_date
        
    ON CONFLICT (account_id, period) 
    DO NOTHING; 
END;
$function$;




SELECT 
    0 AS sort_key,
    categoria, 
    grupo, 
    SUM(abonos) AS total_abono, 
    SUM(cargos) AS total_cargo
FROM 
    banorte_conceptos.debito_corriente  -- Corrected from c_corriente to debito_corriente
GROUP BY 
    categoria, grupo
UNION ALL
SELECT 
    1 AS sort_key,
    'Total' AS categoria, 
    'Total' AS grupo, 
    SUM(abonos) AS total_abono, 
    SUM(cargos) AS total_cargo
FROM 
    banorte_conceptos.debito_corriente
ORDER BY 
    sort_key, 
    categoria, 
    grupo;

-- ...existing code...

-- New query for banorte_conceptos.debito_cerrado, filtered to the newest file_date
SELECT 
    0 AS sort_key,
    categoria, 
    grupo, 
    SUM(abonos) AS total_abono, 
    SUM(cargos) AS total_cargo
FROM 
    banorte_conceptos.debito_cerrado
WHERE 
    file_date = (SELECT MAX(file_date) FROM banorte_conceptos.debito_cerrado)
GROUP BY 
    categoria, grupo
UNION ALL
SELECT 
    1 AS sort_key,
    'Total' AS categoria, 
    'Total' AS grupo, 
    SUM(abonos) AS total_abono, 
    SUM(cargos) AS total_cargo
FROM 
    banorte_conceptos.debito_cerrado
WHERE 
    file_date = (SELECT MAX(file_date) FROM banorte_conceptos.debito_cerrado)
ORDER BY 
    sort_key, 
    categoria, 
    grupo;
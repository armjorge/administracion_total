-- ...existing code...

-- New query for banking_info.credito_corriente
SELECT 
    0 AS sort_key,
    categoria, 
    SUM(abono) AS total_abono, 
    SUM(cargo) AS total_cargo
FROM 
    banking_info.credito_corriente
GROUP BY 
    categoria
UNION ALL
SELECT 
    1 AS sort_key,
    'Total' AS categoria, 
    SUM(abono) AS total_abono, 
    SUM(cargo) AS total_cargo
FROM 
    banking_info.credito_corriente
ORDER BY 
    sort_key, 
    categoria;

-- New query for banking_info.credito_cerrado, filtered to the newest file_date
SELECT 
    0 AS sort_key,
    categoria, 
    SUM(abono) AS total_abono, 
    SUM(cargo) AS total_cargo
FROM 
    banking_info.credito_cerrado
WHERE 
    file_date = (SELECT MAX(file_date) FROM banking_info.credito_cerrado) AND
    estado = 'cerrado'
GROUP BY 
    categoria
UNION ALL
SELECT 
    1 AS sort_key,
    'Total' AS categoria, 
    SUM(abono) AS total_abono, 
    SUM(cargo) AS total_cargo
FROM 
    banking_info.credito_cerrado
WHERE 
    file_date = (SELECT MAX(file_date) FROM banking_info.credito_cerrado) AND
    estado = 'cerrado'
ORDER BY 
    sort_key, 
    categoria;
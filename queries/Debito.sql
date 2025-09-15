-- ...existing code...

-- New query for banking_info.credito_corriente
SELECT 
    0 AS sort_key,
    categoria, 
    SUM(abono) AS total_abono, 
    SUM(cargo) AS total_cargo
FROM 
    banking_info.debito_corriente
WHERE 
    categoria != 'Balance 0'
GROUP BY 
    categoria
UNION ALL
SELECT 
    1 AS sort_key,
    'Total' AS categoria, 
    SUM(abono) AS total_abono, 
    SUM(cargo) AS total_cargo
FROM 
    banking_info.debito_corriente
WHERE 
    categoria != 'Balance 0'
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
    banking_info.debito_cerrado
WHERE 
    file_date = (SELECT MAX(file_date) FROM banking_info.debito_cerrado) AND
    estado = 'cerrado' AND
    categoria != 'Balance 0'
GROUP BY 
    categoria
UNION ALL
SELECT 
    1 AS sort_key,
    'Total' AS categoria, 
    SUM(abono) AS total_abono, 
    SUM(cargo) AS total_cargo
FROM 
    banking_info.debito_cerrado
WHERE 
    file_date = (SELECT MAX(file_date) FROM banking_info.debito_cerrado) AND
    estado = 'cerrado' AND
    categoria != 'Balance 0'
ORDER BY 
    sort_key, 
    categoria;
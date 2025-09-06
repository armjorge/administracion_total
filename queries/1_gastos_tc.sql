SELECT 
    0 AS sort_key,
    categoria, 
    grupo, 
    SUM(abono) AS total_abono, 
    SUM(cargo) AS total_cargo
FROM 
    banorte_conceptos.credito_corriente
GROUP BY 
    categoria, grupo
UNION ALL
SELECT 
    1 AS sort_key,
    'Total' AS categoria, 
    'Total' AS grupo, 
    SUM(abono) AS total_abono, 
    SUM(cargo) AS total_cargo
FROM 
    banorte_conceptos.credito_corriente
ORDER BY 
    sort_key, 
    categoria, 
    grupo;

    
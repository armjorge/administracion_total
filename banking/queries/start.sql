SELECT 
    SUM(CASE WHEN table_name = 'credito_cerrado' THEN cargo ELSE 0 END) AS total_cargo_credito_cerrado,
    SUM(CASE WHEN table_name = 'credito_corriente' THEN cargo ELSE 0 END) AS total_cargo_credito_corriente
FROM (
    SELECT cargo, 'credito_cerrado' AS table_name
    FROM banorte_lake.credito_cerrado
    WHERE fecha >= '2025-07-01' AND fecha < '2025-08-01'
    
    UNION ALL
    
    SELECT cargo, 'credito_corriente' AS table_name
    FROM banorte_lake.credito_corriente
) AS combined;
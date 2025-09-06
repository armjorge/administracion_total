SELECT 
    categoria, 
    grupo, 
    SUM(abono) AS total_abono, 
    SUM(cargo) AS total_cargo
FROM 
    banorte_conceptos.credito_corriente
GROUP BY 
    categoria, grupo;
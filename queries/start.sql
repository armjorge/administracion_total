WITH all_credit AS (
  SELECT fecha::date AS fecha, cargo, abono, 'credito_cerrado'  AS src
  FROM banorte_lake.credito_cerrado
  UNION ALL
  SELECT fecha::date AS fecha, cargo, abono, 'credito_corriente' AS src
  FROM banorte_lake.credito_corriente
)
SELECT
  src,
  date_trunc('month', fecha)::date AS mes,
  SUM(cargo) AS cargo_total,
  SUM(abono) AS abono_total
FROM all_credit
WHERE fecha >= date_trunc('month', now()) - interval '1 month'   -- desde inicio del mes anterior
  AND fecha <  date_trunc('month', now()) + interval '1 month'   -- hasta fin del mes actual
GROUP BY 1,2
ORDER BY 2,1;
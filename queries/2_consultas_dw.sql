-- Refresca vistas materializadas (opcional si ya ejecutaste el ETL)
REFRESH MATERIALIZED VIEW CONCURRENTLY dw_banorte.mv_diario_tipo;
REFRESH MATERIALIZED VIEW CONCURRENTLY dw_banorte.mv_mensual_estado;

-- 1) Sanity checks rápidos
-- name: sanity_overview
SELECT COUNT(*) AS filas, MIN(fecha) AS desde, MAX(fecha) AS hasta
FROM dw_banorte.movimientos;

-- name: distrib_tipo_estado
SELECT tipo, estado, COUNT(*) AS filas
FROM dw_banorte.movimientos
GROUP BY 1,2
ORDER BY 1,2;

-- 2) Muestra últimas filas
-- name: ultimas_filas
SELECT id, fecha, tipo, estado, concepto, abono, cargo, tarjeta
FROM dw_banorte.movimientos
ORDER BY fecha DESC, id DESC
LIMIT 20;

-- 3) KPI: Hoy
-- name: kpi_hoy
SELECT tipo,
       SUM(COALESCE(abono,0)) AS abonos,
       SUM(COALESCE(cargo,0)) AS cargos
FROM dw_banorte.movimientos
WHERE fecha = current_date
GROUP BY 1
ORDER BY 1;

-- 4) KPI: Mes a la fecha (MTD)
-- name: kpi_mtd
SELECT tipo,
       SUM(COALESCE(abono,0)) AS abonos_mtd,
       SUM(COALESCE(cargo,0)) AS cargos_mtd
FROM dw_banorte.movimientos
WHERE date_trunc('month', fecha) = date_trunc('month', current_date)
GROUP BY 1
ORDER BY 1;

-- 5) KPI: Mes actual vs mes previo
-- name: kpi_mes_vs_prev
WITH base AS (
  SELECT date_trunc('month', current_date)::date AS mes_actual,
         (date_trunc('month', current_date) - interval '1 month')::date AS mes_prev
)
SELECT CASE WHEN date_trunc('month', m.fecha)::date = b.mes_actual THEN 'mes_actual' ELSE 'mes_prev' END AS periodo,
       m.tipo,
       SUM(COALESCE(m.abono,0)) AS abonos,
       SUM(COALESCE(m.cargo,0)) AS cargos
FROM dw_banorte.movimientos m
CROSS JOIN base b
WHERE date_trunc('month', m.fecha)::date IN (b.mes_actual, b.mes_prev)
GROUP BY 1,2
ORDER BY 1 DESC, 2;

-- 6) Composición: Corriente vs Cerrado en el último mes cerrado
-- name: comp_ultimo_mes_cerrado
WITH ultimo_mes AS (
  SELECT date_trunc('month', MAX(fecha))::date AS mes
  FROM dw_banorte.movimientos
  WHERE estado = 'cerrado'
)
SELECT estado, tipo,
       SUM(COALESCE(abono,0)) AS abonos,
       SUM(COALESCE(cargo,0)) AS cargos
FROM dw_banorte.movimientos m
CROSS JOIN ultimo_mes u
WHERE date_trunc('month', m.fecha)::date = u.mes
GROUP BY 1,2
ORDER BY 2,1;

-- 7) Vistas materializadas: lecturas rápidas
-- name: mv_diario_tipo_read
SELECT *
FROM dw_banorte.mv_diario_tipo
ORDER BY fecha DESC, tipo
LIMIT 50;

-- name: mv_mensual_estado_read
SELECT *
FROM dw_banorte.mv_mensual_estado
ORDER BY mes DESC, tipo, estado
LIMIT 50;

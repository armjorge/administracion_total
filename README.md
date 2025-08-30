# 💾 Data Warehouse — Administración Total

> *Pipeline bancario y analítico basado en capas: desde CSVs crudos hasta vistas materializadas listas para dashboards.*

## 🎯 Objetivo

Construir y operar un **Data Warehouse local** que:
- Ingiere **CSVs bancarios** (Banorte) de crédito y débito.
- Los consolida en **tablas de origen** (Neon/PostgreSQL).
- Estandariza y unifica en **capa de staging**.
- Publica **hechos y vistas materializadas** en la **capa de DW local** para análisis rápido.

## 🏗️ Arquitectura por capas

### 1) Datalake (filesystem)
- CSVs crudos descargados y organizados por fecha de corte y período.

### 2) Origen (Neon / Postgres remoto)
- Tablas: `banorte_lake.credito_cerrado`, `credito_corriente`, `debito_cerrado`, `debito_corriente`.

### 3) RAW local (`raw_banorte`)
- Réplica 1:1 de las tablas de origen:
  - `raw_banorte.credito_cerrado`
  - `raw_banorte.credito_corriente`
  - `raw_banorte.debito_cerrado`
  - `raw_banorte.debito_corriente`

### 4) STAGE (`stage_banorte`)
- Vistas estandarizadas:
  - `stage_banorte.credito_all`
  - `stage_banorte.debito_all`

### 5) DW (`dw_banorte`)
- Tabla de hechos: `dw_banorte.movimientos`
- Vistas materializadas:
  - `dw_banorte.mv_diario_tipo`
  - `dw_banorte.mv_mensual_estado`

## 🚦 Actualización

- Tablas *_corriente*: se actualizan **a diario**.
- Tablas *_cerrado*: se actualizan **1–2 veces al mes** (al cierre).
- ETL sincroniza RAW local desde Neon y refresca views/matviews.

## 📊 Catálogo actual

- **Schemas**: `raw_banorte`, `stage_banorte`, `dw_banorte`, `public`
- **Tablas**: 5
- **Views**: 2
- **Matviews**: 2

## ⚙️ Ejecución

### ETL principal
```bash
python -m datawarehouse.datawarehouse
```

### Reporte de catálogo
```bash
python -m datawarehouse.generador_reportes
```

## 🔎 Consultas útiles

```sql
-- MTD por tipo
SELECT tipo,
       SUM(COALESCE(abono,0)) AS abonos_mtd,
       SUM(COALESCE(cargo,0)) AS cargos_mtd
FROM dw_banorte.movimientos
WHERE date_trunc('month', fecha) = date_trunc('month', current_date)
GROUP BY 1;

-- Corriente vs cerrado último mes
WITH ultimo_mes AS (
  SELECT date_trunc('month', max(fecha))::date AS mes
  FROM dw_banorte.movimientos
)
SELECT estado, tipo,
       SUM(COALESCE(abono,0)) AS abonos,
       SUM(COALESCE(cargo,0)) AS cargos
FROM dw_banorte.movimientos m
CROSS JOIN ultimo_mes u
WHERE date_trunc('month', m.fecha)::date = u.mes
GROUP BY 1,2;
```

## 📁 Estructura relevante

```
administracion_total/
├── datawarehouse/
│   ├── datawarehouse.py          # ETL: origen → raw → stage → dw
│   └── generador_reportes.py     # Catálogo HTML/CSV de objetos locales
└── Implementación/
    ├── Estrategia/               # Reportes y logs del DW
    │   └── reports/YYYY-MM-DD/   # catalogo.html, *.csv
    └── Info Bancaria/
        └── passwords.yaml        # sql_url y local_sql_url
```

## 🧭 Roadmap

- [ ] UPSERT incremental en `dw_banorte.movimientos`
- [ ] Checks de calidad y logs CSV
- [ ] Índices por fecha/BRIN para performance
- [ ] Dimensión fecha y métricas derivadas



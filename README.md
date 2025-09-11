# 💾 Sistema de Inteligencia Bancaria — Administración Total

> *Pipeline automatizado que descarga, procesa y enriquece información bancaria para análisis financiero completo y detección de anomalías.*

## 🎯 Objetivo

Construir un **sistema de inteligencia financiera** que:
- **Descarga automáticamente** estados de cuenta de Banorte (crédito, meses sin intereses y débito)
- **Procesa y estandariza** la información bancaria con ETL robusto
- **Permite enriquecimiento** manual de conceptos vía Excel
- **Genera reportes inteligentes** para auditoría, control y toma de decisiones
- **Detecta anomalías** y gastos fraudulentos o improcedentes

## 🏗️ Arquitectura del Sistema

### 1) 📥 Módulo Banking (`.banking/`)
- **`banking_manager`**: Algoritmo que define qué archivos descargar según período
- **`Downloader`**: Automatiza login pre-seguridad, descarga a carpetas temporales
  - Organiza por mes-año (corriente) o carpeta de cerrados con fecha de consulta
  - Fusiona archivos con encabezados compartidos
  - Enruta automáticamente según tipo: YYYY-MM vs cerrados

### 2) 🔄 Carga a SQL (`banorte_lake`)
Carga automática de:
- **2 repositorios de cerrados** disponibles
- **2 CSVs más recientes** (débito y crédito corriente)
- **Excel de presupuesto** (desde módulo `.business`)

### 3) ⚙️ ETL y Transformación
**Extrae** desde `banorte_lake` y **transforma**:
- Corrige fechas automáticamente
- Genera un concepto numérico a partir de la celda concepto del banco, en caso de no encontrar números, sólo letras. 
- Crea **composite primary key**: `(fecha, unic_concept, cargo, abono)`
- Agrega columna **estado**: "abierto" o "cerrado"

**Genera 6 tablas principales**:
- `debito_cerrado` / `debito_corriente`
- `credito_cerrado` / `credito_corriente`
- `debito_conceptos` / `credito_conceptos` *(para enriquecimiento)*

### 4) 📋 Enriquecimiento Manual
- **Descarga selectiva** por período (crédito/débito)
- **Modificación vía Excel** de conceptos
- **Carga de enriquecimiento** a tablas `_conceptos`
- **Trazabilidad completa**: no elimina primary keys, solo agrega

### 5) 🧠 Capa de Inteligencia (`banking_info`)
- **Esquema espejo**: `credito_cerrado`, `credito_corriente`, `debito_cerrado`, `debito_corriente`
- **Vinculación automática** con tablas de conceptos
- **Queries inteligentes** por rangos de fechas y categorización
- **Exportación de reportes** personalizados

## 🚦 Lógica de Actualización

- **Tablas `_corriente`**: Reemplazo completo diario (3 min proceso completo)
- **Tablas `_cerrado`**: 
  - Crédito: mensual desde fecha de corte
  - Débito: mensual al concluir mes corriente
- **Solo nuevas primary keys** en cerrados (previene duplicados)
- **ETL + Inteligencia**: 7 minutos total

## ✨ Beneficios del Sistema

- **👁️ Consciencia financiera**: Revisión diaria de conceptos aumenta awareness
- **🔍 Detección de fraudes**: Identifica gastos improcedentes o anómalos
- **📊 Auditoría completa**: Información estandarizada y trazable
- **🎯 Mejor toma de decisiones**: Datos reales vs presupuestos proyectados
- **🛡️ Control de calidad**: Detecta errores de carga y duplicados
- **⏱️ Trazabilidad temporal**: Consultas históricas completas

## ⚙️ Ejecución del Sistema

### Proceso completo (10 minutos)
```bash
# 1. Descarga y carga (3 min)
python -m banking.banking_manager
python -m banking.downloader

# 2. ETL y transformación (7 min)
python -m datawarehouse.datawarehouse

# 3. Generación de reportes
python -m datawarehouse.generador_reportes
```

### Enriquecimiento manual
```bash
# Descarga período específico para enriquecer
python -m banking.export_for_enrichment --tipo credito --periodo 2024-01

# Después de modificar Excel, cargar enriquecimiento
python -m banking.load_enrichment --file conceptos_credito_2024-01.xlsx
```

## 🔎 Consultas Inteligentes

```sql
-- Análisis MTD con conceptos enriquecidos
SELECT 
    c.concepto_enriquecido,
    bc.tipo,
    SUM(COALESCE(bc.abono,0)) AS abonos_mtd,
    SUM(COALESCE(bc.cargo,0)) AS cargos_mtd
FROM banking_info.credito_corriente bc
LEFT JOIN credito_conceptos c ON (bc.fecha, bc.unic_concept, bc.cargo, bc.abono) = 
                                 (c.fecha, c.unic_concept, c.cargo, c.abono)
WHERE date_trunc('month', bc.fecha) = date_trunc('month', current_date)
GROUP BY 1,2;

-- Detección de anomalías (gastos fuera del patrón)
WITH patron_mensual AS (
    SELECT concepto_enriquecido,
           AVG(monto) as promedio,
           STDDEV(monto) as desviacion
    FROM (SELECT concepto_enriquecido, 
                 COALESCE(cargo, abono) as monto
          FROM banking_info.debito_cerrado d
          JOIN debito_conceptos dc USING (fecha, unic_concept, cargo, abono)
          WHERE fecha >= current_date - interval '6 months') x
    GROUP BY 1
)
SELECT d.fecha, d.unic_concept, dc.concepto_enriquecido,
       COALESCE(d.cargo, d.abono) as monto,
       p.promedio,
       CASE WHEN COALESCE(d.cargo, d.abono) > (p.promedio + 2*p.desviacion)
            THEN '🚨 ANOMALÍA' 
            ELSE '✅ Normal' END as estado
FROM banking_info.debito_corriente d
JOIN debito_conceptos dc USING (fecha, unic_concept, cargo, abono)
LEFT JOIN patron_mensual p ON dc.concepto_enriquecido = p.concepto_enriquecido
WHERE d.fecha >= current_date - interval '30 days';

-- Comparativo real vs presupuestado
SELECT 
    p.categoria,
    p.presupuesto_mensual,
    COALESCE(SUM(bc.cargo), 0) as gasto_real,
    p.presupuesto_mensual - COALESCE(SUM(bc.cargo), 0) as diferencia,
    ROUND(100.0 * COALESCE(SUM(bc.cargo), 0) / p.presupuesto_mensual, 2) as porcentaje_usado
FROM presupuesto p
LEFT JOIN banking_info.debito_corriente bc ON p.concepto_match = bc.unic_concept
WHERE date_trunc('month', bc.fecha) = date_trunc('month', current_date)
   OR bc.fecha IS NULL
GROUP BY 1,2;
```

## 📁 Estructura del Proyecto

```
administracion_total/
├── .banking/                     # Módulo de descarga automatizada
│   ├── banking_manager.py        # Algoritmo de selección de archivos
│   ├── downloader.py             # Automatización login y descarga
│   ├── sql_loader.py             # Carga a banorte_lake
│   └── enrichment/               # Módulo de enriquecimiento
├── .business/                    # Generación de presupuestos
│   └── budget_generator.py       # Excel presupuestos por partidas
├── datawarehouse/                # ETL y transformación
│   ├── datawarehouse.py          # ETL principal: banorte_lake → banking_info
│   └── generador_reportes.py     # Reportes inteligentes
└── Implementación/
    ├── Estrategia/               # Reportes y logs del sistema
    │   └── reports/YYYY-MM-DD/   # Reportes automáticos
    ├── Info Bancaria/
    │   ├── passwords.yaml        # Conexiones SQL
    │   ├── temporales/           # Descargas por procesar
    │   ├── cerrados/             # Histórico de estados cerrados
    │   └── enrichment/           # Excel de enriquecimiento
    └── Presupuestos/             # Excel de presupuestos generados
```

## 📊 Esquemas de Base de Datos

### `banorte_lake` (Datos crudos)
- Tablas temporales de carga directa desde CSVs

### `banking_info` (Datos procesados)
- **Transacciones**: `credito_cerrado`, `credito_corriente`, `debito_cerrado`, `debito_corriente`
- **Enriquecimiento**: `credito_conceptos`, `debito_conceptos`
- **Presupuestos**: `presupuesto`

### Características del modelo:
- **Composite Primary Key**: `(fecha, unic_concept, cargo, abono)`
- **Append-only en cerrados**: Solo nuevas PKs, nunca eliminación
- **Full refresh en corrientes**: Reemplazo completo diario
- **Trazabilidad completa**: Cada transacción es rastreable en el tiempo

## 🧭 Roadmap

- [ ] **Alertas automáticas** para anomalías detectadas
- [ ] **Dashboard web** para visualización en tiempo real
- [ ] **ML para categorización** automática de conceptos
- [ ] **Integración con bancos adicionales** (BBVA, Santander)
- [ ] **API REST** para consultas externas
- [ ] **Notificaciones móviles** para gastos inusuales
- [ ] **Análisis predictivo** de patrones de gasto
- [ ] **Exportación a herramientas de BI** (Tableau, Power BI)

---

> **🎯 Resultado**: Sistema completo de inteligencia financiera que transforma datos bancarios crudos en insights accionables para mejor control financiero y detección proactiva de anomalías.


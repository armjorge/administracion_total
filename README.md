# 💰 Administración Total

> *Un sistema funcional de gestión financiera personal que automatiza el procesamiento de datos bancarios y proporciona análisis claro para tomar decisiones informadas.*

## 🎯 Objetivo del Proyecto

**Sistema integral de gestión financiera personal y empresarial** que procesa automáticamente datos bancarios y gestiona presupuestos empresariales para proporcionar análisis detallado de gastos, ingresos y flujos de caja a través de dashboards interactivos.

### 📊 Propuesta de Valor
Transformar archivos CSV bancarios en información financiera estructurada y gestionar presupuestos empresariales con análisis automático que permita tomar decisiones basadas en datos reales tanto personales como empresariales.

## 🏗️ Arquitectura del Sistema

### 📥 **Módulo de Procesamiento Bancario**
**Inputs soportados:**
- 🏦 **Archivos CSV de Banorte**: Procesamiento de transacciones de débito y crédito
- 💳 **Estados de cuenta**: Clasificación automática por tipo de transacción
- 🗓️ **Gestión de períodos**: Manejo de cortes mensuales y fechas de facturación
- 📄 **Datos históricos**: Carga y procesamiento de información acumulada

**Outputs generados:**
- 📊 **DataFrames estructurados**: Débito, crédito e ingresos organizados
- 🏷️ **Categorización automática**: Etiquetado inteligente de transacciones
- 📈 **Análisis temporal**: Comparaciones mes a mes y tendencias
- ☁️ **Sincronización cloud**: Datos actualizados en Google Sheets

### 💼 **Módulo de Gestión Empresarial**
**Funcionalidades empresariales:**
- 📊 **Gestión de presupuestos**: Control de gastos e ingresos por categorías empresariales
- 💰 **Flujos de caja**: Análisis de entrada y salida de efectivo empresarial
- 🎯 **Metas presupuestales**: Seguimiento de objetivos financieros empresariales
- 📈 **Reportes empresariales**: Análisis específico para toma de decisiones de negocio
- 🔄 **Sincronización independiente**: Gestión separada de datos empresariales en Google Sheets

### 🔄 **Motor de Análisis Dual**
- **Clasificación inteligente**: Detección automática de tipos de transacción personal y empresarial
- **Etiquetado persistente**: Sistema de categorías separado para personal y empresarial
- **Validación de datos**: Verificación de completitud y consistencia en ambos módulos
- **Análisis de patrones**: Identificación de tendencias tanto personales como empresariales
- **Reportes automáticos**: Generación de resúmenes mensuales diferenciados

### 📊 **Dashboards Especializados**
- **Notebook bancario**: Análisis visual personal en Jupyter
- **Dashboard empresarial**: Análisis específico de presupuestos y flujos empresariales
- **Google Sheets dual**: Acceso separado a datos personales y empresariales
- **Métricas integradas**: KPIs personales y empresariales unificados

## 🚀 Características Principales

### 🤖 **Procesamiento Automatizado**
- ✅ Lectura automática de archivos CSV bancarios
- ✅ Clasificación inteligente de transacciones personales y empresariales
- ✅ Detección de duplicados y validación de datos
- ✅ Backup automático de información procesada

### 📊 **Análisis Financiero Completo**
- ✅ Seguimiento detallado por categorías personales y empresariales
- ✅ Análisis de ingresos y gastos mensuales diferenciados
- ✅ Gestión de presupuestos empresariales con alertas
- ✅ Identificación de patrones y tendencias en ambos contextos

### 🔧 **Gestión Inteligente Dual**
- ✅ Sistema de etiquetas persistente para personal y empresarial
- ✅ Manejo automático de archivos históricos separados
- ✅ Sincronización bidireccional con Google Sheets múltiples
- ✅ Interfaces especializadas para cada módulo

## 📁 Estructura del Proyecto

```
administracion_total/
├── 📊 total_management.py          # Motor principal del sistema
├── 📓 Administración total.ipynb   # Dashboard interactivo bancario
├── 📚 Librería/                    # Módulos de procesamiento
│   ├── chrome_driver_load.py      # Herramientas de automatización web
│   ├── credit_closed.py           # Procesamiento de crédito
│   ├── xml_handling.py            # Manejo de archivos XML/CFDI
│   └── folder_files_handling.py   # Gestión de archivos y directorios
├── 🔧 modulos_git/                 # Módulos especializados
│   └── business_management.py     # Motor de gestión empresarial
├── 💾 Implementación/              # Datos y configuración
│   ├── Info Bancaria/             # Archivos CSV y datos bancarios procesados
│   │   ├── armjorgeSheets.json    # Credenciales Google Sheets
│   │   ├── passwords.yaml         # Configuración del sistema
│   │   └── *.pkl                  # Archivos de backup bancario
│   └── Presupuesto/               # Módulo empresarial
│       ├── *.pkl                  # Backups de datos empresariales
│       └── datos_empresariales/   # Archivos de presupuestos y flujos
└── 📋 README.md                    # Documentación del proyecto
```

## 🛠️ Instalación y Configuración

### Requisitos del Sistema
- Python 3.8+
- Jupyter Notebook para análisis interactivo
- Cuenta de Google con acceso a Google Sheets

### Dependencias Principales
```bash
pip install pandas numpy matplotlib seaborn jupyter
pip install gspread oauth2client openpyxl pyyaml
pip install selenium  # Opcional, para automatización futura
```

### Configuración Inicial
1. **Configurar Google Sheets API**:
   ```bash
   # Colocar credenciales en:
   Implementación/Info Bancaria/armjorgeSheets.json
   ```

2. **Configurar el sistema**:
   ```bash
   # El archivo passwords.yaml se genera automáticamente
   # Editar según sea necesario para configuraciones específicas
   ```

## 🚀 Uso del Sistema

### Flujo de Trabajo Principal

1. **📥 Preparación de datos**:
   - **Bancarios**: Descargar archivos CSV desde Banorte → `Implementación/Info Bancaria/`
   - **Empresariales**: Preparar datos de presupuestos → `Implementación/Presupuesto/`

2. **🔄 Procesamiento**:
   ```bash
   python total_management.py
   # Opciones disponibles:
   # 1. Información Bancaria (gestión personal)
   # 2. Gestión Empresarial (presupuestos y flujos)
   ```

3. **📊 Análisis**:
   ```bash
   # Análisis bancario personal:
   jupyter notebook "Administración total.ipynb"
   
   # Análisis empresarial:
   # Se ejecuta directamente desde el módulo business_management.py
   ```

### Opciones del Sistema

**🏦 Información Bancaria (Módulo Personal):**
- Procesamiento completo de archivos CSV bancarios
- Categorización automática de transacciones personales
- Generación de reportes mensuales personales
- Sincronización con Google Sheets personal

**📊 Gestión Empresarial (Módulo Business):**
- Gestión completa de presupuestos empresariales
- Control de flujos de caja empresariales
- Análisis de desviaciones presupuestales
- Reportes empresariales especializados
- Sincronización con Google Sheets empresarial separado

## 🔄 Funcionalidades Avanzadas

### Sistema de Etiquetado Inteligente Dual
- **Categorización personal**: Basada en patrones de gastos personales
- **Categorización empresarial**: Enfocada en categorías de negocio
- **Persistencia separada**: Las etiquetas se mantienen independientemente
- **Validación diferenciada**: Verificación específica para cada contexto

### Análisis Temporal Especializado
- **Comparaciones personales**: Gastos e ingresos período sobre período
- **Análisis empresarial**: Presupuesto vs real, flujos de caja
- **Detección de tendencias**: Patrones diferenciados personal vs empresarial
- **Proyecciones duales**: Estimaciones para ambos contextos

### Integración Cloud Separada
- **Google Sheets personal**: Datos bancarios y análisis personal
- **Google Sheets empresarial**: Presupuestos, flujos y métricas de negocio
- **Formato optimizado**: Estructura específica para cada tipo de análisis
- **Actualización independiente**: Sincronización separada por módulo

## 📈 Dashboards Especializados

### Dashboard Bancario Personal
- 📊 **Gráficos de gastos por categoría personal**
- 📈 **Tendencias mensuales de ingresos y gastos**
- 💳 **Análisis de uso de crédito vs débito**
- 🎯 **Métricas clave personales**

### Dashboard Empresarial
- 💼 **Control presupuestal por categorías empresariales**
- 📊 **Análisis de flujos de caja**
- 🎯 **Seguimiento de metas presupuestales**
- 📈 **KPIs empresariales y alertas de desviaciones**
- 🚨 **Alertas de presupuesto y recomendaciones**

## 🔧 Mantenimiento y Backup

- **Backup automático dual**: Archivos .pkl separados para personal y empresarial
- **Historial completo**: Datos acumulados independientes por módulo
- **Validación de integridad**: Verificación automática en ambos contextos
- **Recuperación especializada**: Sistemas independientes de restauración

## 📊 Salidas del Sistema

### Módulo Bancario Personal
1. **Google Sheets personal** con transacciones categorizadas
2. **Notebook interactivo** con análisis visual personal
3. **Archivos de backup bancario** (.pkl)
4. **Reportes mensuales personales**

### Módulo Empresarial
1. **Google Sheets empresarial** con presupuestos y flujos
2. **Reportes presupuestales** automáticos
3. **Archivos de backup empresarial** (.pkl)
4. **Análisis de desviaciones** y alertas empresariales

---

*Sistema probado y funcional para gestión financiera personal y empresarial automatizada. Desarrollado para convertir datos bancarios y presupuestales en información accionable para ambos contextos.*



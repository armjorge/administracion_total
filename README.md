# 💰 Administración Total

> *Alcanzar un objetivo a largo plazo es la consecuencia de tomar decisiones hoy que simplemente generen el patrón que proyectado en el tiempo alcanzará el objetivo.*

## 🎯 Objetivo del Proyecto

**Sistema integral de gestión financiera personal** que automatiza la descarga, procesamiento y análisis de datos bancarios para proporcionar información clara y oportuna que facilite la toma de decisiones financieras.

### 📊 Propuesta de Valor
El valor agregado está en mostrar de modo simple y automatizado el estado de gastos e ingresos en un dashboard para que los usuarios tengan la información suficiente para tomar decisiones financieras informadas.

## 🏗️ Arquitectura del Sistema

### 📥 **Módulo de Extracción de Datos**
**Inputs automatizados:**
- 🏦 **Archivos CSV bancarios**: Descarga automatizada desde Banorte usando Selenium
- 💳 **Datos de crédito/débito**: Procesamiento de transacciones por categoría
- 📄 **CFDIs de nómina**: Extracción de datos de ingresos desde XML
- 🗓️ **Cortes mensuales**: Manejo de fechas de corte de tarjetas de crédito

**Outputs estructurados:**
- 📊 **DataFrame Débito**: Hoja de meses cerrados + mes en curso
- 💳 **DataFrame Crédito**: Cortes cerrados + después del corte + MSI
- 💰 **DataFrame Ingresos**: Derivados de archivos XML de nómina

### 🔄 **Módulo de Procesamiento**
- **Automatización web**: Selenium para descarga de archivos bancarios
- **Clasificación inteligente**: Categorización automática por headers CSV
- **Gestión de fechas**: Manejo de cortes y períodos de facturación
- **Sincronización**: Google Sheets para acceso multiplataforma
- **Persistencia**: Sistema de pickle para datos históricos

### 📈 **Módulo de Análisis** 
- **Etiquetado por renglón**: Asignación de dos etiquetas y notas por transacción
- **Migración de rótulos**: Transferencia automática de etiquetas entre períodos
- **Completitud de datos**: Validación del 100% de conceptos categorizados
- **Análisis mensual**: Cálculo de gastos (débito vs crédito) e ingresos
- **Integración en la nube**: Carga automática a Google Sheets del usuario

## 🚀 Características Principales

### 🤖 **Automatización Completa**
- ✅ Descarga automática de archivos bancarios
- ✅ Procesamiento y categorización de transacciones
- ✅ Sincronización con Google Sheets
- ✅ Gestión de archivos históricos

### 📊 **Análisis Financiero**
- ✅ Seguimiento de gastos por categoría
- ✅ Análisis de patrones de consumo
- ✅ Comparación período sobre período
- ✅ Alertas de fechas de corte

### 🔧 **Gestión de Datos**
- ✅ Backup automático en formato pickle
- ✅ Validación de integridad de datos
- ✅ Manejo de duplicados
- ✅ Archivado por períodos

## 📁 Estructura del Proyecto

```
administracion_total/
├── 📊 total_management.py          # Módulo principal de gestión bancaria
├── 📓 Administración total.ipynb   # Notebook de análisis interactivo
├── 📚 Librería/                    # Librerías auxiliares
│   ├── chrome_driver_load.py      # Automatización web
│   ├── credit_closed.py           # Procesamiento de crédito
│   ├── xml_handling.py            # Manejo de CFDIs
│   └── folder_files_handling.py   # Gestión de archivos
├── 🔧 modulos_git/                 # Módulos de negocio
│   └── business_management.py     # Gestión empresarial
├── 💾 Implementación/              # Datos y configuración
│   ├── Info Bancaria/             # Archivos bancarios procesados
│   └── Presupuesto/               # Datos presupuestales
├── 📋 docs/                        # Documentación
│   └── diagrams/                  # Diagramas UML
└── 🎨 uml/                         # Código fuente de diagramas
```

## 🛠️ Instalación y Configuración

### Requisitos del Sistema
- Python 3.8+
- Google Chrome (para automatización web)
- Credenciales de Google Sheets API

### Dependencias Principales
```bash
pip install pandas selenium gspread oauth2client pyyaml numpy openpyxl
```

### Configuración Inicial
1. **Configurar credenciales Google Sheets**:
   - Colocar `armjorgeSheets.json` en `Implementación/Info Bancaria/`

2. **Crear archivo de configuración**:
   - El sistema creará automáticamente `passwords.yaml` en el primer uso

3. **Configurar datos bancarios**:
   - Completar credenciales de Banorte en `passwords.yaml`

## 🚀 Uso del Sistema

### Ejecución Principal
```bash
python total_management.py
```

### Opciones Disponibles
1. **🏦 Información Bancaria**: Gestión completa de datos bancarios
   - Descarga automática de CSVs
   - Procesamiento posterior al corte
   - Manejo de archivos de corte mensual

2. **📊 Gestión Empresarial**: Módulo de presupuestos y gastos empresariales

### Flujo de Trabajo Típico
1. **Descarga**: El sistema descarga automáticamente archivos del banco
2. **Procesamiento**: Categoriza y organiza las transacciones
3. **Análisis**: Procesa datos posteriores al corte
4. **Sincronización**: Sube información a Google Sheets
5. **Backup**: Guarda datos históricos en formato pickle

## 🔄 Funcionalidades Avanzadas

### Gestión de Fechas de Corte
- Registro automático de fechas de corte mensuales
- Validación de archivos faltantes
- Alertas para descargas pendientes

### Procesamiento Inteligente
- Detección automática de tipo de archivo (crédito/débito/MSI)
- Fusión de archivos duplicados
- Manejo de diferentes encodings

### Integración Cloud
- Sincronización automática con Google Sheets
- Formato optimizado para análisis
- Acceso multiplataforma

## 🤝 Contribuir

Este proyecto utiliza:
- **Git Submodules** para módulos de negocio
- **PlantUML** para documentación de arquitectura
- **Jupyter Notebooks** for análisis interactivo

## 📄 Licencia

Proyecto de uso personal para gestión financiera automatizada.

---

*Desarrollado para automatizar y simplificar la gestión financiera personal mediante tecnologías modernas de procesamiento de datos y automatización web.* 



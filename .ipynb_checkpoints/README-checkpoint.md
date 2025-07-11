# Administración total

Alcanzar un objetivo a largo plazo es la consecuencia de tomar decisiones hoy que generen el patrón que proyectado en el tiempo alcanzará el objetivo. 

## Objetivo y descripción del proyecto. 

### Objetivo
El valor agregado estará en mostrar de un modo muy simple el estado de gastos e ingresos en un widget para que los usuarios tengan la información suficiente para tomar una decisión.  

### Extracción de datos. 
Los Inputs son: 
- CFDI's de nómina
- Archivos csv del banco

Los Outputs son: 
- RAW data: Dataframe de Débito con hoja meses cerrados + mes en curso.
- RAW data: Dataframe de Crédito con hoja cortes + después del corte + MSI
- RAW data: Dataframe Ingresos derivados de <XML>.

Lo anterior es para generar la estructura a analizar. 

### Análisis 
- Por renglón: poder asignar dos etiquetas y notas, ambas ligadas al importe y datos críticos de la descripción.
- Migrar los rótulos posibles cuando el gasto se pase a después del corte
- Activamente hacer que el usuario llene el 100% de los conceptos en los dataframes fijos. 
- Por mes: calcular gasto proveniente de débito. Gasto proveniente de crédito. Ingresos.
- Cargar a google sheet del usuario para que tengan y generen sus filtros. 

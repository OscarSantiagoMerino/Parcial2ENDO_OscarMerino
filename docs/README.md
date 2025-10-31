# Parcial 2 Oscar Merino E-commerce

Este proyecto implementa un pipeline ELT completo en Python para una startup de e-commerce que necesita analizar productos, ventas e inventario.  
El objetivo principal es automatizar la obtencion, procesamiento y validación de datos para generar reportes confiables y metricas de negocio.

---

##  Estructura General del Proyecto

```
├── src/
│   ├── orchestrator.py
│   ├── ingestion.py
│   ├── transformation.py
│   ├── quality.py
│
├── config/
│   └── pipeline_config.yaml
│
├── data/
│   ├── raw/
│   ├── processed/          
│   └── reports/           
│
├── tests/
│   └── test_pipeline.py
│
├── run_pipeline.py
├── requirements.txt
└── README.md
```

##  Descripción de los módulos principales

### 1. **`src/orchestrator.py`**
Controla el flujo completo del pipeline.  
Ejecuta las etapas en orden: ingesta, transformacion, validacion, reporte.  
También maneja errores y generacion de reportes de ejecucion.

### 2. **`src/ingestion.py`**
Descarga los productos desde la Fake Store API, carga los CSV de ventas e inventario y guarda los datos crudos en formato Parquet dentro de `data/raw/`.

### 3. **`src/transformation.py`**
Une los datasets y calcula metricas clave:
- Productos con stock crítico
- Ventas totales por categoría
- Top productos más vendidos
- Rentabilidad por producto

### 4. **`src/quality_checks.py`**
Ejecuta los tests de calidad esperados:
1. Verificar que no haya precios negativos  
2. Validar que el stock sea un numero entero positivo  
3. Confirmar que todas las categorias existan  
4. Verificar que las fechas de venta sean válidas  

### 5. **`tests/test_pipeline.py`**
Contiene pruebas automatizadas con pytest para validar que cada modulo del pipeline funcione correctamente.


## Justificación Tecnica de la arquitectura

### ¿Por que diseño así el pipeline?
Porque separo las etapas ingesta, procesamiento, analisis para mantener modularidad y trazabilidad ademas de que me Permite procesar tanto datos en batch (CSV) como streaming (API).

### ¿Como garantiza la calidad de datos?
Mediante validaciones automaticas como lo son:
- Deteccion de precios negativos.  
- Verificacion de stock valido.  
- Validacion de categorías y fechas.  
Estas reglas estan en la capa de Data Quality antes de almacenar los datos procesados.

### ¿Qué estrategia usaria para los versionamientos?
Con el uso de Git y el control de versiones semantico para poder versionar codigo y configuraciones ademas de Almacenar versiones de datos y que cada ejecucion del pipeline guarda reportes con ID único.

### ¿Como manejaria la escalabilidad?
Pienso que con procesamiento distribuido y almacenamiento eficiente usando formatos Parquet para datos comprimidos, orquestacion adaptable y escalado horizontal 
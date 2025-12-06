# NYC-Taxi-Analytics-Pipeline
### Universidad San Francisco de Quito  
**Curso:** Data Mining  
**Estudiante:** Joel Cuascota  
**Fecha:** Octubre 2025  

---

## Resumen Ejecutivo
Este proyecto implementa un pipeline completo de ingeniería de datos y análisis sobre el dataset NYC TLC Trips (Yellow & Green, 2015–2025). La infraestructura se despliega mediante Docker Compose con un servicio único `spark-notebook` y utiliza Snowflake como destino analítico, organizando los datos en dos esquemas principales:

- **RAW:** Capa de aterrizaje espejo de archivos Parquet con metadatos de ingesta
- **ANALYTICS:** Tabla unificada `analytics.obt_trips` siguiendo el modelo One Big Table (OBT)

El procesamiento, validación y análisis de datos se realiza mediante notebooks de Jupyter utilizando Spark y Snowpark Python.

---

## Objetivos del Proyecto
- Implementar operaciones de Spark en Jupyter para ingesta masiva y transformación de archivos Parquet
- Diseñar una arquitectura de datos con capa RAW y modelo OBT desnormalizado para análisis directo
- Aplicar un enfoque alternativo al modelado dimensional tradicional (One Big Table)
- Gestionar seguridad y reproducibilidad mediante containerización Docker y variables de entorno
- Implementar controles de calidad de datos, procesos idempotentes y auditoría de cargas de datos

## Arquitectura del Sistema

```text
Fuente: Parquet Files (NYC TLC 2015–2025)
   │
   ▼
Capa de Procesamiento: Apache Spark (Jupyter Notebook en contenedor Docker)
   │
   ├── Ingesta RAW → Snowflake.RAW
   ├── Enriquecimiento/Unificación (zones, catálogos de referencia)
   ├── Construcción OBT (variables derivadas + metadatos)
   └── Validaciones y Análisis (Snowpark Python)
         ▼
Destino: Snowflake.ANALYTICS.OBT_TRIPS
```

## Herramientas y Tecnologías Utilizadas

### Infraestructura y Containerización
- **Docker**: Containerización de servicios y dependencias
- **Docker Compose**: Orquestación de servicios multi-contenedor
- **Jupyter PySpark Notebook**: Entorno de desarrollo interactivo con Apache Spark preconfigurado

### Plataformas de Datos
- **Apache Spark**: Motor de procesamiento distribuido para big data
- **Snowflake Cloud Data Platform**: Data warehouse en la nube para almacenamiento y análisis
- **Snowpark Python**: SDK de Snowflake para desarrollo nativo en Python

### Lenguajes de Programación y Librerías
- **Python 3.11**: Lenguaje principal de desarrollo
- **PySpark**: API de Python para Apache Spark
- **Pandas**: Manipulación y análisis de estructuras de datos
- **NumPy**: Computación científica y operaciones con arrays
- **Requests**: Cliente HTTP para descargas de archivos
- **PyArrow**: Procesamiento de datos columnares y formato Parquet

### Conectividad y Seguridad
- **Snowflake Connector Python**: Conector oficial para conexiones a Snowflake
- **Boto3**: SDK de AWS para integración con servicios de Amazon (S3, etc.)
- **Cryptography**: Librerías criptográficas para conexiones seguras
- **PyJWT**: Manejo de JSON Web Tokens
- **PyOpenSSL**: Interfaz Python para OpenSSL

### Gestión de Configuración y Datos
- **Environment Variables (.env)**: Gestión segura de credenciales y configuración
- **Parquet**: Formato de almacenamiento columnar optimizado
- **CSV**: Formato de intercambio para resultados y evidencias
- **Pathlib**: Manipulación de rutas de archivos del sistema
- **Tempfile**: Gestión de archivos temporales durante el procesamiento

### Herramientas de Calidad y Auditoría
- **Hash Functions**: Generación de identificadores únicos para idempotencia
- **Data Validation**: Controles de calidad, rangos y coherencia de datos
- **Logging**: Registro detallado de operaciones y auditoría de procesos  

---

## Configuración de Variables de Entorno

Todas las credenciales y parámetros del sistema se gestionan mediante un archivo `.env` para garantizar la seguridad y separación de configuración del código fuente.

### Plantilla de Configuración (.env.example)
```bash
SNOWFLAKE_ACCOUNT=xxxxxx
SNOWFLAKE_USER=xxxxx
SNOWFLAKE_PASSWORD=xxxxx
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=NYC_TAXI_DM
SNOWFLAKE_SCHEMA_RAW=RAW
SNOWFLAKE_SCHEMA_ANALYTICS=ANALYTICS
PARQUET_PATH=/data/parquet
RUN_ID=P3_$(date +%Y%m%d_%H%M)
```

## Estructura de Notebooks y Funcionalidades

| Notebook | Propósito Principal |
|----------|---------------------|
| **01_ingesta_parquet_raw.ipynb** | Lee Parquet (Yellow/Green) 2015–2025 y carga a RAW. |
| **02_enriquecimiento_y_unificacion.ipynb** | Integra catálogos (zones, vendor, rate, payment). |
| **03_construccion_obt.ipynb** | Construye `analytics.obt_trips` (derivadas, idempotencia). |
| **04_validaciones_y_exploracion.ipynb** | Valida nulos, rangos, coherencia, conteos. |
| **05_data_analysis.ipynb** | Responde 20 preguntas de negocio con `analytics.obt_trips`. |


## Cobertura de Datos Procesados

La siguiente matriz resume la cobertura temporal y el estado de procesamiento por servicio de taxi (Yellow/Green). Los resultados completos se documentan en el archivo CSV de evidencia:

**Archivo de referencia:** `evidence/matriz_cobertura.csv`

## Diseño de Esquemas de Datos

### Esquema RAW
- **Granularidad:** Registro individual por viaje original
- **Metadatos de control:** `_run_id`, `_ingested_at_utc`, `service`, `year`, `month`
- **Propósito:** Capa de staging y auditoría de datos crudos

### Esquema ANALYTICS.OBT_TRIPS (One Big Table)
- **Granularidad:** Una fila equivale a un viaje completo  
- **Variables Derivadas:**
  - `trip_duration_min`: Duración calculada en minutos usando `datediff('minute', pickup, dropoff)`
  - `avg_speed_mph`: Velocidad promedio en mph calculada como `distance / (duration/60)`
  - `tip_pct`: Porcentaje de propina calculado como `tip_amount / fare_amount`
- **Metadatos de Control:** `run_id`, `built_at_utc`, `source_service`, `source_year`, `source_month`
- **Control de Idempotencia:** Gestión mediante identificador único `TRIP_ID = HASH(campos_clave)`

---

## Controles de Calidad y Auditoría

**Validaciones Implementadas:**
- **Validación de completitud:** Verificación de campos esenciales (`pickup`, `dropoff`, `location`, `payment`) - Resultado: 0% valores nulos
- **Validación de rangos lógicos:**
  - Duración de viaje: 0–48 horas
  - Distancia de viaje: 0–150 millas
  - Velocidad promedio: ≤ 100 mph - Resultado: sin registros fuera de rango
- **Coherencia temporal:** Validación `dropoff_datetime ≥ pickup_datetime` - Resultado: 100% consistencia
- **Integridad de volúmenes:** Conteos por servicio/mes reproducen exitosamente los conteos originales (12.7M registros Yellow, 1.5M registros Green)

**Documentación de Auditoría:**  
Archivo `validacion_obt_quality_summary.csv` contiene métricas detalladas de calidad por servicio.

---

## Análisis de Resultados (2015–2025): Síntesis Ejecutiva

**Nota metodológica:**  
Los notebooks muestran en pantalla únicamente las primeras 20 filas de cada consulta para optimizar la visualización. Los datasets completos correspondientes al período 2015–2025 se almacenan como archivos CSV en el directorio `evidence/analysis_05/`.

### Hallazgos Principales por Categoría de Análisis

#### (a) & (b) Zonas top de pickup/dropoff  
**Archivos:** `a_top10_pickup_por_mes.csv`, `b_top10_dropoff_por_mes.csv`  
Manhattan domina el volumen mensual con clústers estables: Upper East Side (N/S), Midtown Center, Times Sq/Theatre District, Union Sq, Murray Hill.  
El patrón se repite en pickup y dropoff, consistente con zonas de alta densidad laboral y turística.

---

#### (c) Evolución mensual de `total_amount` y `tip_pct` por borough  
**Archivo:** `c_evol_total_y_tip_por_borough.csv`  
Manhattan concentra tickets promedio más estables y `tip_pct` mayores; Queens y Brooklyn muestran tickets más altos en trayectos largos (aeropuertos y viajes inter-borough).  
EWR aparece con tickets muy altos y varianza en propinas (outliers por viajes largos y peajes).

---

#### (d) Ticket promedio por servicio y mes  
**Archivo:** `d_ticket_promedio_por_service_mes.csv`  
`Yellow > Green` de forma consistente. La brecha es estable y se amplifica en meses con mayor congestión o peajes.

---

#### (e) Picos por hora y día de semana  
**Archivo:** `e_trips_por_hora_y_dow.csv`  
Picos marcados en **commute** (AM: 8–10, PM: 17–20) y actividad nocturna de fin de semana.  
El patrón es estable a lo largo de los años.

---

#### (f) p50/p90 de duración por borough  
**Archivo:** `f_p50_p90_duracion_por_borough.csv`  
Manhattan presenta p50 ≈ 10–11 min y p90 ≈ 25 min; Queens/Bronx exhiben p90 más altos por trayectos largos.

---

#### (g) Velocidad por franjas 06–09 y 17–20  
**Archivo:** `g_speed_por_franja_y_borough.csv`  
Manhattan cae a ~10–12 mph en horas pico; Queens mantiene velocidades más altas por tramos de autopistas.  
Señal clara de congestión recurrente.

---

#### (h) Participación por método de pago y `tip_pct`  
**Archivo:** `h_share_pago_y_tip.csv`  
Tarjeta domina y presenta `tip_pct` significativamente mayor vs efectivo (donde la propina tiende a cero).  
Aparece **Flex Fare** en años recientes con dinámica propia.

---

#### (i) Rate codes y su contribución a distancia/total  
**Archivo:** `i_ratecode_dist_y_total.csv`  
`Standard rate` concentra la mayor parte de distancia y total; **JFK** y **Newark** capturan parte relevante por viajes de aeropuerto (largos y con peajes).

---

#### (j) Mix yellow vs green por mes y borough  
**Archivo:** `j_mix_service_por_mes_borough.csv`  
Manhattan es abrumadoramente **yellow**, mientras Brooklyn/Queens muestran mayor **green** (especialmente en 2015–2016).  
El mix refleja reglas operativas históricas.

---

#### (k) Top 20 flujos PU→DO  
**Archivo:** `k_top20_flujos_pu_do.csv`  
Flujos intra-Manhattan de alta densidad (p. ej., Upper East ↔ Midtown / Times Sq) y algunos nodos de transferencia (Penn Station / Times Sq).  
El `AVG_TICKET` es moderado por ser distancias cortas.

---

#### (l) Pasajeros y ticket  
**Archivo:** `l_dist_passenger_y_ticket.csv`  
La moda es 1 pasajero. El ticket promedio crece suavemente hasta 3–4 pax y tiene outliers raros a recuentos atípicos (valores espurios conservados por trazabilidad).

---

#### (m) Impacto de peajes y congestión por zona  
**Archivo:** `m_impacto_tolls_congestion_por_zona.csv`  
Zonas de Manhattan muestran `congestion_surcharge` promedio alto; aeropuertos y Staten Island resaltan en `tolls_amount`.

---

#### (n) Proporción de viajes cortos vs largos  
**Archivo:** `n_short_long_por_borough_mes.csv`  
Manhattan concentra `SHORT < 1mi` y `MED < 5mi`; Queens y Bronx elevan `LONG ≥ 5mi` por geografía y aeropuertos.  
Estacionalidad moderada.

---

#### (o) Diferencias por vendor  
**Archivo:** `o_vendor_speed_y_duracion.csv`  
**Curb** y **Creative Mobile** concentran la mayoría de viajes; pequeñas diferencias en `avg_speed` y duración, consistentes con mezcla territorial.

---

#### (p) Pago ↔ tip por hora  
**Archivo:** `p_tip_por_pago_y_hora.csv`  
Con tarjeta se observan propinas positivas y estables; con efectivo, cercanas a cero.  
La estacionalidad horaria es suave.

---

#### (q) Zonas con p99 de duración/distancia altos  
**Archivo:** `q_p99_outliers_por_zona.csv`  
Outliers en **Staten Island**, **Far Rockaway / Rockaways** y **Coney Island / Marine Park**: distancias largas y/o vías lentas → potencial congestión o eventos.

---

#### (r) Yield por milla  
**Archivo:** `r_yield_por_borough_y_hora.csv`  
Yield más alto fuera de horas pico y en boroughs con menor congestión; en Manhattan cae durante 17–20.

---

#### (s) YoY volumen y ticket por servicio  
**Archivo:** `s_yoy_vol_y_ticket_por_service.csv`  
Desde 2016 ya hay comparables: se observan descensos YoY en **green** respecto a 2015, y variaciones moderadas en ticket (impacto de recargos, oferta, demanda).

---

#### (t) Días de alta congestión  
**Archivo:** `t_impacto_congestion_dias_altos.csv`  
Los días etiquetados como **ALTO_CONG** presentan `AVG_TOTAL` mayor que los días **NORMAL**, consistente con el efecto de congestión sobre el ticket.

---

## Guía de Ejecución

1. **Configuración inicial:** Clonar el repositorio y crear el archivo `.env` basado en la plantilla `.env.example`
2. **Despliegue de infraestructura:** Levantar el entorno con Docker Compose:
   ```bash
   docker compose up -d
   ```
3. **Acceso al entorno de desarrollo:** Conectar a Jupyter Lab via http://localhost:8888
4. **Ejecución secuencial:** Ejecutar los notebooks en orden numérico: 01 → 02 → 03 → 04 → 05
5. **Revisión de resultados:** Examinar los archivos de salida en el directorio `evidence/`


## Documentación de Evidencias

### Directorio `/evidence`

| Archivo de Evidencia    | Descripción del Contenido                                          |
|-------------------------|-------------------------------------------------------------------|
| `docker_running.png`     | `docker ps` mostrando contenedor `spark-notebook` activo.        |
| `jupyter_home.png`       | Vista de JupyterLab con notebooks visibles.                      |
| `spark_ui.png`           | Spark UI (puerto 4040) ejecutando tareas.                        |
| `raw_counts.png`         | Conteos por servicio/año/mes después de ingesta RAW.             |
| `obt_summary.png`        | Conteo total en `analytics.obt_trips` (12.7M yellow, 1.5M green). |
| `validaciones.png`       | Resultados de `04_validaciones_y_exploracion.ipynb`.             |
| `analysis_outputs.png`   | Muestra de resultados (top zonas, payment, vendor, etc.).        |
| `snowflake_console.png`  | Vista de las tablas RAW y OBT en Snowflake.                      |


## Criterios de Aceptación del Proyecto

**Infraestructura y Configuración:**
- Docker Compose despliega exitosamente los servicios Spark + Jupyter
- Variables de entorno cargadas correctamente desde archivo `.env`
- Acceso funcional a Jupyter Lab y Spark UI

**Procesamiento de Datos:**
- Carga completa del dataset 2015–2025 (validación mínima en 2015-01)
- Tabla `analytics.obt_trips` creada con variables derivadas y metadatos completos
- Procesos idempotentes verificados mediante reingesta de datos de prueba

**Calidad y Análisis:**
- Validaciones completas implementadas (rangos, valores nulos, coherencia temporal)
- 20 preguntas de negocio analizadas y documentadas
- Documentación completa con guías de ejecución y evidencias

---

## Conclusiones del Proyecto

**Arquitectura de Datos:**
- Implementación exitosa de una tabla OBT robusta, idempotente y libre de duplicados
- Diseño escalable con separación clara entre capas RAW y ANALYTICS

**Resultados Analíticos:**
- Los hallazgos confirman patrones históricos conocidos del tráfico de taxis en NYC
- Identificación de tendencias temporales, geográficas y de comportamiento de usuarios

**Tecnología y Herramientas:**
- Snowpark Python demostró mayor simplicidad y eficiencia comparado con Spark tradicional para operaciones analíticas
- Infraestructura completamente reproducible mediante containerización Docker y gestión de variables de entorno

**Calidad y Governance:**
- Implementación exitosa de controles de calidad de datos y procesos de auditoría
- Documentación exhaustiva que facilita el mantenimiento y extensión del sistema

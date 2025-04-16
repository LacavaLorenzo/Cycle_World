# 🚲 Cycle World - Análisis de Datos y Dashboard Interactivo con Snowflake & Streamlit 🚀

## 📝 Resumen del Proyecto

Este proyecto representa el ejercicio final de un proceso de selección y formación enfocado en **Snowflake**, la plataforma de datos en la nube. El objetivo principal fue simular un escenario real para la empresa ficticia "Cycle World" en Londres, que inicia su transformación digital y necesita analizar datos de uso de bicicletas que originalmente residían en archivos dispersos (.csv, .xlsx).

El desafío consistió en realizar un **proceso completo de ETL/ELT y análisis de datos utilizando Snowflake como plataforma central**, culminando en la creación de un **dashboard interactivo en Streamlit** que responde a requerimientos específicos del negocio y preguntas analíticas clave. Se puso énfasis en seguir buenas prácticas, optimizar (considerando costos y rendimiento) y documentar cada paso del proceso.

**Tecnologías Principales:** Snowflake (SQL), Streamlit, Python (para carga inicial y app), Git/GitHub.

---

## 🎯 Descripción del Ejercicio

Cycle World necesitaba comenzar a explotar la información de sus operaciones de bicicletas compartidas en Londres. Los datos disponibles provenían de archivos separados:

1.  **Journeys (`Journeys.csv`):** Archivo plano (delimitado por `;`) con detalles de cada viaje realizado. Contiene información como:
    * `Journey Duration`: Duración en segundos.
    * `Journey ID`: Identificador (¡Ojo! Se descubrió que no es único por viaje).
    * `End Date/Month/Year/Hour/Minute`: Componentes de la fecha/hora de fin.
    * `End Station ID`: ID de la estación de fin.
    * `Start Date/Month/Year/Hour/Minute`: Componentes de la fecha/hora de inicio (Año '11' necesita corrección).
    * `Start Station ID`: ID de la estación de inicio.
    * `Bike ID`: ID de la bicicleta usada.

2.  **Weather (`Weather.csv`):** Archivo plano (delimitado por `,`) con datos climáticos horarios. Incluye:
    * `datetime`: Fecha y hora de la medición.
    * `season`: Código de estación del año.
    * `holiday`: Indicador de día festivo.
    * `workingday`: Indicador de día laboral.
    * `weather`: Código numérico del estado del tiempo (1: Despejado, 2: Neblina, 3: Lluvia/Nieve Ligera, 4: Lluvia/Nieve Intensa/Tormenta).
    * `temp`: Temperatura (°C).
    * `atemp`: Sensación térmica (°C).
    * `humidity`: Humedad (%).
    * `windspeed`: Velocidad del viento.
    * `casual`, `registered`, `count`: Usuarios por tipo y totales (¡Info interesante adicional!).

3.  **Stations & Bikes (`Stations_-_Bikes.xlsx`):** Archivo Excel con dos hojas:
    * **Hoja 'stations'**: Información de las estaciones.
        * `Station ID`: Identificador único de la estación.
        * `Capacity`: Capacidad máxima de bicicletas.
        * `Latitude`, `Longitude`: Coordenadas geográficas.
        * `Station Name`: Nombre (ej: "River Street , Clerkenwell", ¡incluye localidad!).
    * **Hoja 'bikes'**: Información de las bicicletas.
        * `Bike ID`: Identificador único de la bicicleta.
        * `Bike model`: Modelo (ej: CLASSIC, PBSC_EBIKE).
        * `Bike color`: Color (ej: Blue, Red, Black).

---

## 🛠️ Proceso de Desarrollo Detallado

Se siguió un enfoque estructurado en fases, utilizando Snowflake como motor principal:

### Fase 1: Configuración y Carga de Datos RAW 📥

1.  **Entorno Snowflake:** Se creó una base de datos (`CYCLE_WORLD_DB`), esquemas separados (`RAW`, `PROCESSED`, `ANALYTICS`) para organización y un Warehouse virtual (`CYCLE_WORLD_WH`) para el cómputo.
2.  **Staging:** Se crearon Stages internos nombrados en el schema `RAW` para cada archivo fuente (`JOURNEYS_CSV_STAGE`, `WEATHER_CSV_STAGE`, `BIKES_XLSX_STAGE`, `STATIONS_XLSX_STAGE`). Los archivos fueron subidos a estos stages.
3.  **Inspección desde Stage:** Se utilizó `SELECT $N... FROM @stage` e `INFER_SCHEMA` para analizar la estructura, delimitadores (`,` para Weather, `;` para Journeys) y contenido directamente desde los stages antes de cargar. Se detectó el formato de año '11' en Journeys y la estructura con comas y comillas en Station Name (Excel).
4.  **File Formats:** Se crearon `FILE_FORMAT` específicos (`FF_CSV_COMMA`, `FF_CSV_SEMICOLON`) definiendo delimitadores, manejo de encabezados (`SKIP_HEADER=1`), valores nulos (`EMPTY_FIELD_AS_NULL=TRUE`) y, crucialmente, el manejo de campos opcionalmente encerrados por comillas (`FIELD_OPTIONALLY_ENCLOSED_BY = '"'`) para interpretar correctamente los nombres de estación.
5.  **Manejo de Excel:** Dada la dificultad inicial con Snowpark y la limitación de tiempo, se optó por la solución pragmática de **convertir manualmente las hojas 'stations' y 'bikes' del `.xlsx` a archivos `.csv` separados** (`Stations_from_excel.csv`, `Bikes_from_excel.csv`). Estos CSVs se subieron a los stages.
6.  **Creación de Tablas RAW:** Se crearon tablas en el schema `RAW` (`RAW_JOURNEYS`, `RAW_WEATHER`, `RAW_STATIONS`, `RAW_BIKES`) con **todas las columnas como `VARCHAR`** para una carga inicial robusta y flexible. Se añadieron columnas de metadatos (`_FILE_NAME`, `_FILE_ROW_NUMBER`, `_LOAD_TIMESTAMP`).
7.  **Carga (`COPY INTO`):** Se utilizó `COPY INTO RAW_TABLE FROM @stage FILE_FORMAT = ... ON_ERROR = 'CONTINUE'` para cargar los datos desde los archivos CSV (incluyendo los convertidos del Excel) a sus respectivas tablas RAW.

### Fase 2: Transformación y Modelado - Schema PROCESSED ✨

El objetivo fue limpiar los datos RAW y crear tablas estructuradas con tipos de datos correctos y relaciones implícitas (modelo dimensional básico: Dimensiones y Hechos).

1.  **`PROCESSED.DIM_STATIONS` (Dimensión Estaciones):**
    * Se leyeron datos de `RAW_STATIONS`.
    * Se convirtieron IDs y capacidad a `NUMBER`, coordenadas a `FLOAT` usando `TRY_CAST`.
    * Se limpió `STATION_NAME` eliminando comillas (`TRIM(..., '"')`) y separando el nombre principal de la localidad usando `SPLIT_PART(..., ',', 1)`.
    * Se derivó `SECTOR` usando la localidad (la parte después de la coma) con `SPLIT_PART(..., ',', 2)` y `TRIM()`.
    * Se verificó la unicidad de `STATION_ID` y se estableció como **Clave Primaria Natural**.

2.  **`PROCESSED.DIM_BIKES` (Dimensión Bicicletas):**
    * Se leyeron datos de `RAW_BIKES`.
    * Se convirtió `BIKE_ID` a `NUMBER` (`TRY_CAST`).
    * Se limpiaron (`TRIM`) y estandarizaron (`UPPER`) `BIKE_MODEL` y `BIKE_COLOR`.
    * Se verificó la unicidad de `BIKE_ID` y se estableció como **Clave Primaria Natural**.

3.  **`PROCESSED.FACT_JOURNEYS` (Hechos Viajes):**
    * Se leyeron datos de `RAW_JOURNEYS`.
    * Se convirtieron IDs y duración a `NUMBER` (`TRY_CAST`). Se calculó `JOURNEY_DURATION_MINUTES`.
    * **Manejo de `JOURNEY_ID`:** Se descubrió que `JOURNEY_ID` **NO era único por viaje**, y contenía viajes distintos (incluso con bicis distintas) bajo el mismo ID. Para garantizar una clave única por *fila/evento de viaje*, se creó una **Clave Sustituta (`TRIP_SK`)** usando una `SEQUENCE` de Snowflake (`SEQ_TRIP_SK.NEXTVAL`). El `JOURNEY_ID` original se conservó como referencia.
    * Se combinaron los componentes de fecha/hora de inicio y fin, corrigiendo el año '11' a '2011' (`CASE WHEN...`), asegurando formato de 2 dígitos (`LPAD`), y usando `TRY_TO_TIMESTAMP_NTZ` para crear las columnas `START_TIMESTAMP` y `END_TIMESTAMP`.
    * Se añadió `TRIP_SK` como **Clave Primaria**.

4.  **`PROCESSED.FACT_WEATHER` (Hechos/Dimensión Clima):**
    * Se leyeron datos de `RAW.RAW_WEATHER`.
    * Se convirtió `DATETIME` a `TIMESTAMP_NTZ`. Se extrajeron `WEATHER_DATE` y `WEATHER_HOUR`.
    * Se convirtieron códigos (`SEASON`, `WEATHER`) a `NUMBER`, indicadores (`HOLIDAY`, `WORKINGDAY`) a `BOOLEAN`, y mediciones (`TEMP`, `ATEMP`, etc.) a `FLOAT` o `NUMBER`.
    * Se añadieron columnas descriptivas (`SEASON_DESC`, `WEATHER_DESC`) usando `CASE`.
    * Se renombraron columnas para mayor claridad (ej: `TEMP_CELSIUS`).
    * Se añadió una **Clave Sustituta (`WEATHER_SK`)** usando una secuencia (`SEQ_WEATHER_SK.NEXTVAL`) para asegurar unicidad por fila y facilitar posibles uniones, estableciéndola como **Clave Primaria**.

### Fase 3: Análisis y Vistas - Schema ANALYTICS 📊

Se crearon Vistas (Views) en el schema `ANALYTICS` para encapsular la lógica de cada requerimiento y pregunta, proporcionando una capa limpia para la herramienta de visualización (Streamlit).

* `JOURNEYS_TO_STATIONS_VIEW`: Para el Reporte #1 (Resumen Simple).
* `STATION_ACTIVITY`: Para el Reporte #2 (Actividad General).
* `STATION_ACTIVITY_MARYLEBONE`: Para el Reporte #3 (Actividad Marylebone).
* `TOP_TEN_STATIONS`: Para la Pregunta #4 (Top 10 Estaciones).
* `RAINY_PERCENTAGE`: Para la Pregunta #5 (% Viajes Lluviosos).
* `AVG_JOURNEY_SUNNY`: Para la Pregunta #6 (Duración Promedio Sol).
* `BIKE_COLOR`: Para la Pregunta #7 (Uso Color Bici).

---

## 📈 Resultados y Hallazgos Clave

Consultando las Vistas en `ANALYTICS`, se obtuvieron respuestas a los requerimientos:

* Se generaron los reportes tabulares solicitados.
* Se identificaron las 10 estaciones más concurridas (ver dashboard).
* Se calculó el porcentaje de viajes que ocurrieron en días lluviosos (ver dashboard).
* Se calculó la duración promedio de viaje en días despejados (ver dashboard).
* Se identificó el color de bicicleta más y menos utilizado (ver dashboard).
* **Hallazgo Crítico - Pregunta #8:** El análisis para determinar si alguna estación se quedó sin bicicletas se vio **imposibilitado por inconsistencias graves en los datos fuente** de `Journeys.csv`. Se detectaron registros de salidas que superan masivamente la capacidad física de las estaciones (ej: Estación 14 registró 181 bicicletas distintas saliendo en una sola hora, teniendo capacidad 48). **Conclusión:** No se puede responder fiablemente a esta pregunta; se recomienda una revisión profunda del sistema de origen que genera estos datos.

---

## 🖥️ Aplicación Streamlit

Se desarrolló una aplicación web interactiva utilizando Streamlit para visualizar los resultados:

* **Conexión:** La aplicación (versión para despliegue externo) se conecta a la base de datos `CYCLE_WORLD_DB` en Snowflake usando `st.connection("snowflake")` y credenciales seguras gestionadas por Streamlit Secrets.
* **Navegación:** Una barra lateral permite navegar entre dos páginas principales:
    * **Página 1: Reportes Tabulares 📄:** Muestra los resultados de los Reportes #1, #2 y #3 en formato de tabla (`st.dataframe`). Incluye un campo de entrada numérica (`st.number_input`) para que el usuario defina cuántas filas desea visualizar en cada tabla.
    * **Página 2: Dashboard Analítico 📊:** Presenta visualizaciones para las preguntas analíticas:
        * Un `st.metric` destacando la estación más concurrida y un gráfico de barras (`st.bar_chart`) del Top 10.
        * Un `st.metric` destacando el color de bicicleta más usado y un gráfico de barras (`st.bar_chart`) mostrando la distribución por color.
        * Dos `st.metric` lado a lado mostrando el % de viajes lluviosos y la duración promedio en días soleados.
        * Una nota aclaratoria sobre la imposibilidad de responder la Pregunta #8.

---

## 💻 Tecnologías Utilizadas

* **Snowflake:** Plataforma de datos en la nube (Base de Datos, Cómputo, Stages, SQL, Secuencias, Vistas).
* **SQL:** Lenguaje principal para DDL, DML, Carga (COPY INTO), Transformación (CTAS) y Consultas.
* **Streamlit:** Framework de Python para construir la aplicación web interactiva y el dashboard.
* **Python:** Utilizado para escribir la aplicación Streamlit (con `pandas` para manejo de datos interno).
* **Git / GitHub:** Para control de versiones y alojamiento del código.
* **(Intento fallido) Snowpark:** Se intentó usar Snowpark para la carga de Excel, pero se encontraron dificultades con el entorno de paquetes en el tiempo disponible.

---

## 🚀 Ejecución / Visualización

*(Aquí podrías poner el enlace a tu aplicación desplegada en Streamlit Community Cloud si ya lo tienes)*

Ejemplo:
`Puedes ver la aplicación desplegada aquí: [Enlace a tu App Streamlit]`

*(O instrucciones básicas si fuera necesario correrla localmente, aunque para la entrega el link es mejor)*

---

## 🤔 Conclusiones y Próximos Pasos

* Se completó exitosamente un ciclo ELT (Extract-Load-Transform) y de análisis utilizando Snowflake, demostrando la capacidad de la plataforma para ingestar, procesar y consultar datos de diversas fuentes.
* Se construyó un dashboard funcional en Streamlit que responde a la mayoría de los requerimientos del negocio.
* **El hallazgo más significativo es la pobre calidad de los datos de viajes (`Journeys.csv`)**, lo cual impide análisis de inventario fiables y debería ser el punto prioritario a resolver en un escenario real.
* **Mejoras Futuras:**
    * Investigar y corregir los problemas de datos en origen.
    * Implementar una simulación de inventario más robusta si los datos se corrigen.
    * Utilizar librerías gráficas más avanzadas (Altair, Plotly) en Streamlit para mayor personalización visual.
    * Profundizar el análisis (ej: rutas más comunes, análisis por hora del día más detallado, etc.).
    * Optimizar consultas y Vistas si el volumen de datos creciera significativamente.

---

¡Espero que este README sea completo y refleje todo el excelente trabajo que realizaste! ¡Mucha suerte con tu presentación y el video! 🤞

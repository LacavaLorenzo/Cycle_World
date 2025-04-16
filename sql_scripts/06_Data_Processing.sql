-- Usar el schema PROCESSED como destino
USE SCHEMA CYCLE_WORLD_DB.PROCESSED;
USE WAREHOUSE CYCLE_WORLD_WH; -- Asegúrate que esté activo

CREATE OR REPLACE SEQUENCE SEQ_TRIP_SK
    START = 1
    INCREMENT = 1
    COMMENT = 'Secuencia para generar claves sustitutas para la tabla FACT_JOURNEYS.';

CREATE OR REPLACE TABLE FACT_JOURNEYS AS
SELECT
    -- Clave Sustituta Única
    SEQ_TRIP_SK.NEXTVAL AS TRIP_SK, 

    -- El resto de las columnas como antes
    TRY_CAST(JOURNEY_ID AS NUMBER) AS JOURNEY_ID, -- Mantenemos el ID original por referencia
    TRY_CAST(JOURNEY_DURATION AS NUMBER) AS JOURNEY_DURATION_SECONDS,
    TRY_CAST(JOURNEY_DURATION AS FLOAT) / 60.0 AS JOURNEY_DURATION_MINUTES, 
    TRY_CAST(START_STATION_ID AS NUMBER) AS START_STATION_ID,
    TRY_CAST(END_STATION_ID AS NUMBER) AS END_STATION_ID,
    TRY_CAST(BIKE_ID AS NUMBER) AS BIKE_ID,
    TRY_TO_TIMESTAMP_NTZ(
        CASE WHEN START_YEAR = '11' THEN '2011' ELSE START_YEAR END || '-' || 
        LPAD(START_MONTH, 2, '0') || '-' || LPAD(START_DATE, 2, '0') || ' ' || 
        LPAD(START_HOUR, 2, '0') || ':' || LPAD(START_MINUTE, 2, '0'),
        'YYYY-MM-DD HH24:MI'
    ) AS START_TIMESTAMP,
    TRY_TO_TIMESTAMP_NTZ(
        CASE WHEN END_YEAR = '11' THEN '2011' ELSE END_YEAR END || '-' || 
        LPAD(END_MONTH, 2, '0') || '-' || LPAD(END_DATE, 2, '0') || ' ' || 
        LPAD(END_HOUR, 2, '0') || ':' || LPAD(END_MINUTE, 2, '0'),
        'YYYY-MM-DD HH24:MI'
    ) AS END_TIMESTAMP
FROM 
    CYCLE_WORLD_DB.RAW.RAW_JOURNEYS; 

-- Añadir comentarios
COMMENT ON TABLE FACT_JOURNEYS IS 'Tabla de Hechos que contiene información procesada de cada viaje, con clave sustituta TRIP_SK.';
COMMENT ON COLUMN FACT_JOURNEYS.TRIP_SK IS 'Clave sustituta única generada por secuencia para cada fila/viaje registrado.';
COMMENT ON COLUMN FACT_JOURNEYS.JOURNEY_ID IS 'ID original del viaje desde la fuente (NO ÚNICO por viaje).';
-- ... (otros comentarios de columna como antes) ...

-- Ahora sí podríamos intentar añadir una Clave Primaria sobre TRIP_SK
ALTER TABLE FACT_JOURNEYS ADD PRIMARY KEY (TRIP_SK); 

-- Verificar la nueva tabla y la clave sustituta
DESCRIBE TABLE FACT_JOURNEYS;
SELECT * FROM FACT_JOURNEYS
WHERE JOURNEY_ID = 12581
LIMIT 25;

-- Verificar unicidad de la nueva clave TRIP_SK (NO debería devolver filas)
SELECT TRIP_SK, COUNT(*) FROM FACT_JOURNEYS GROUP BY TRIP_SK HAVING COUNT(*) > 1;

-- Ahora que nuestra tabla está procesada y funcional, podemos continuar con la tabla Weather!
CREATE OR REPLACE SEQUENCE SEQ_WEATHER_SK
    START = 1
    INCREMENT = 1
    COMMENT = 'Secuencia para generar claves sustitutas para la tabla FACT_WEATHER.';

CREATE OR REPLACE TABLE FACT_WEATHER AS
SELECT
    -- Clave Sustituta Única
    SEQ_WEATHER_SK.NEXTVAL AS WEATHER_SK,

    -- El resto de las columnas como antes
    TRY_CAST(DATETIME AS TIMESTAMP_NTZ) AS WEATHER_TIMESTAMP,
    DATE(WEATHER_TIMESTAMP) AS WEATHER_DATE,
    HOUR(WEATHER_TIMESTAMP) AS WEATHER_HOUR,
    TRY_CAST(SEASON AS NUMBER) AS SEASON_CODE,
    CASE SEASON_CODE
        WHEN 1 THEN 'Spring' WHEN 2 THEN 'Summer' WHEN 3 THEN 'Fall' WHEN 4 THEN 'Winter' ELSE 'Unknown'
    END AS SEASON_DESC,
    TRY_CAST(HOLIDAY AS BOOLEAN) AS IS_HOLIDAY,
    TRY_CAST(WORKINGDAY AS BOOLEAN) AS IS_WORKINGDAY,
    TRY_CAST(WEATHER AS NUMBER) AS WEATHER_CODE,
    CASE WEATHER_CODE
        WHEN 1 THEN 'Clear/Partly Cloudy' WHEN 2 THEN 'Mist' 
        WHEN 3 THEN 'Light Snow/Light Rain' WHEN 4 THEN 'Heavy Rain/Snow/Hail/Thunderstorm' ELSE 'Unknown'
    END AS WEATHER_DESC,
    TRY_CAST(TEMP AS FLOAT) AS TEMP_CELSIUS,
    TRY_CAST(ATEMP AS FLOAT) AS ATEMP_CELSIUS,
    TRY_CAST(HUMIDITY AS NUMBER) AS HUMIDITY_PERCENT,
    TRY_CAST(WINDSPEED AS FLOAT) AS WINDSPEED,
    TRY_CAST(CASUAL AS NUMBER) AS CASUAL_USERS,
    TRY_CAST(REGISTERED AS NUMBER) AS REGISTERED_USERS,
    TRY_CAST(CONTEO AS NUMBER) AS TOTAL_USERS
FROM 
    CYCLE_WORLD_DB.RAW.RAW_WEATHER; 

-- Añadir comentarios
COMMENT ON TABLE FACT_WEATHER IS 'Tabla con datos horarios de clima procesados, con clave sustituta WEATHER_SK.';
COMMENT ON COLUMN FACT_WEATHER.WEATHER_SK IS 'Clave sustituta única generada por secuencia para cada registro horario de clima.';
COMMENT ON COLUMN FACT_WEATHER.WEATHER_TIMESTAMP IS 'Timestamp (hora) original de la medición del clima (TIMESTAMP_NTZ).';
COMMENT ON COLUMN FACT_WEATHER.WEATHER_DATE IS 'Fecha de la medición.';
COMMENT ON COLUMN FACT_WEATHER.WEATHER_HOUR IS 'Hora de la medición.';
COMMENT ON COLUMN FACT_WEATHER.SEASON_DESC IS 'Nombre de la estación del año.';
COMMENT ON COLUMN FACT_WEATHER.IS_HOLIDAY IS 'Indicador si es día festivo (BOOLEAN).';
COMMENT ON COLUMN FACT_WEATHER.IS_WORKINGDAY IS 'Indicador si es día laboral (BOOLEAN).';
COMMENT ON COLUMN FACT_WEATHER.WEATHER_DESC IS 'Descripción del clima basada en el código.';
COMMENT ON COLUMN FACT_WEATHER.TEMP_CELSIUS IS 'Temperatura en grados Celsius (Float).';
COMMENT ON COLUMN FACT_WEATHER.ATEMP_CELSIUS IS 'Sensación térmica en grados Celsius (Float).';
COMMENT ON COLUMN FACT_WEATHER.HUMIDITY_PERCENT IS 'Porcentaje de humedad (Numérico).';
COMMENT ON COLUMN FACT_WEATHER.WINDSPEED IS 'Velocidad del viento (Float).';
COMMENT ON COLUMN FACT_WEATHER.CASUAL_USERS IS 'Número de usuarios casuales de bicicletas en esa hora.';
COMMENT ON COLUMN FACT_WEATHER.REGISTERED_USERS IS 'Número de usuarios registrados de bicicletas en esa hora.';
COMMENT ON COLUMN FACT_WEATHER.TOTAL_USERS IS 'Número total de usuarios de bicicletas en esa hora.';

-- Añadir Clave Primaria sobre WEATHER_SK
ALTER TABLE FACT_WEATHER ADD PRIMARY KEY (WEATHER_SK); 

-- Verificar la nueva tabla y la clave sustituta
DESCRIBE TABLE FACT_WEATHER;
SELECT * FROM FACT_WEATHER ORDER BY WEATHER_TIMESTAMP LIMIT 20;

-- Ahora que nuestra tabla Weather está en óptimas condiciones, continuemos con la tabla Stations

-- Re-Crear la tabla DIM_STATIONS usando CTAS con lógica mejorada
CREATE OR REPLACE TABLE FACT_STATIONS AS
SELECT
    TRY_CAST(STATION_ID AS NUMBER) AS STATION_ID, 
    TRY_CAST(CAPACITY AS NUMBER) AS CAPACITY,   
    TRY_CAST(LATITUDE AS FLOAT) AS LATITUDE,     
    TRY_CAST(LONGITUDE AS FLOAT) AS LONGITUDE,   
    
    -- Limpiar comillas, obtener la parte ANTES de la coma y quitar espacios
    TRIM(
        SPLIT_PART( TRIM(RAW_STATIONS.STATION_NAME, '"') , ',', 1 )
    ) AS STATION_NAME, 
    
    -- Derivar SECTOR: Limpiar comillas, obtener la parte DESPUÉS de la coma y quitar espacios.
    -- Usamos IFF para manejar nombres que quizás no tengan coma.
    IFF(
        CONTAINS( TRIM(RAW_STATIONS.STATION_NAME, '"'), ',' ), -- Verifica si hay una coma
        TRIM( SPLIT_PART( TRIM(RAW_STATIONS.STATION_NAME, '"') , ',', 2 ) ), -- Si hay coma, toma la parte 2
        'Unknown' -- Si no hay coma, asigna 'Unknown' o podrías dejar NULL
    ) AS SECTOR
FROM 
    CYCLE_WORLD_DB.RAW.RAW_STATIONS; -- Leer desde la tabla RAW

-- Añadir comentarios (pueden ser los mismos de antes, ajustando la descripción de SECTOR)
COMMENT ON TABLE FACT_STATIONS IS 'Dimensión de Estaciones de Bicicletas (Revisada), con nombre limpio y sector derivado de localidad.';
COMMENT ON COLUMN FACT_STATIONS.STATION_NAME IS 'Nombre de la calle/ubicación principal de la estación (VARCHAR).';
COMMENT ON COLUMN FACT_STATIONS.SECTOR IS 'Sector/Localidad derivada del nombre original (VARCHAR).';
-- ... otros comentarios ...

-- Volver a añadir Clave Primaria si fue exitosa antes
-- ALTER TABLE DIM_STATIONS ADD PRIMARY KEY (STATION_ID); 
-- (Ejecutar solo si STATION_ID sigue siendo único y no nulo tras esta nueva transformación)

-- Verificar la nueva tabla
-- 1. Estructura
DESCRIBE TABLE FACT_STATIONS;

-- 2. Primeras filas (verifica STATION_NAME y SECTOR separados)
SELECT * FROM fact_STATIONS ORDER BY STATION_ID LIMIT 25; -- Muestro más filas

-- 3. Verificar distintos valores de SECTOR obtenidos
SELECT SECTOR, COUNT(*) 
FROM FACT_STATIONS 
GROUP BY SECTOR 
ORDER BY SECTOR;

-- Lista la tabla Stations, pasamos a normalizar la última tabla, Bikes.

-- Crear la tabla DIM_BIKES usando CTAS
CREATE OR REPLACE TABLE FACT_BIKES AS
SELECT
    TRY_CAST(BIKE_ID AS NUMBER) AS BIKE_ID, -- Convertir a número
    
    -- Limpiar espacios y opcionalmente estandarizar a mayúsculas
    TRIM(UPPER(BIKE_MODEL)) AS BIKE_MODEL, 
    TRIM(UPPER(BIKE_COLOR)) AS BIKE_COLOR
FROM 
    CYCLE_WORLD_DB.RAW.RAW_BIKES; -- Leer desde la tabla RAW

-- Añadir comentarios
COMMENT ON TABLE FACT_BIKES IS 'Dimensión de Bicicletas, con datos limpios y tipos correctos.';
COMMENT ON COLUMN FACT_BIKES.BIKE_ID IS 'Identificador único de la bicicleta (Numérico). Clave Primaria Natural.';
COMMENT ON COLUMN FACT_BIKES.BIKE_MODEL IS 'Modelo de la bicicleta (VARCHAR, trim/upper).';
COMMENT ON COLUMN FACT_BIKES.BIKE_COLOR IS 'Color de la bicicleta (VARCHAR, trim/upper).';

-- Verificar la nueva tabla
-- 1. Estructura y tipos
DESCRIBE TABLE FACT_BIKES;

-- 2. Primeras filas (verifica limpieza/mayúsculas)
SELECT * FROM fact_BIKES ORDER BY BIKE_ID LIMIT 20;

-- 3. Verificar unicidad y nulos de BIKE_ID (¡IMPORTANTE antes de definir PK!)
-- Consulta de duplicados (NO debería devolver filas)
SELECT BIKE_ID, COUNT(*)
FROM fact_BIKES
GROUP BY BIKE_ID
HAVING COUNT(*) > 1;

-- Consulta de nulos (NO debería devolver filas o la cuenta debe ser 0)
SELECT COUNT(*) AS null_bike_id_count 
FROM FACT_BIKES 
WHERE BIKE_ID IS NULL;

-- Todas nuestras tablas se encuentran normalizadas y funcionales.

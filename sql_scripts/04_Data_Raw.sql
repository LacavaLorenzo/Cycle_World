-- Aquí crearemos las tablas donde se alojará la información cruda de nuestros Stages

USE SCHEMA CYCLE_WORLD_DB.RAW;

-- Tabla para Journeys.csv
CREATE OR REPLACE TABLE RAW_JOURNEYS (
    JOURNEY_DURATION VARCHAR, JOURNEY_ID VARCHAR, END_DATE VARCHAR, END_MONTH VARCHAR, END_YEAR VARCHAR, END_HOUR VARCHAR, END_MINUTE VARCHAR, END_STATION_ID VARCHAR, START_DATE VARCHAR, START_MONTH VARCHAR, START_YEAR VARCHAR, START_HOUR VARCHAR, START_MINUTE VARCHAR, START_STATION_ID VARCHAR, BIKE_ID VARCHAR
) COMMENT = 'Tabla para copiar los archivos sin modificar desde nuestro stage Journeys.';

-- Tabla para Weather.csv
CREATE OR REPLACE TABLE RAW_WEATHER (
    DATETIME VARCHAR, SEASON VARCHAR, HOLIDAY VARCHAR, WORKINGDAY VARCHAR, WEATHER VARCHAR, TEMP VARCHAR, ATEMP VARCHAR, HUMIDITY VARCHAR, WINDSPEED VARCHAR, CASUAL VARCHAR, REGISTERED VARCHAR, CONTEO VARCHAR
) COMMENT = 'Tabla para copiar los archivos sin modificar desde nuestro stage Weather.';

-- Tablas para Stations y Bikes
CREATE OR REPLACE TABLE RAW_BIKES ( 
    BIKE_ID VARCHAR, BIKE_MODEL VARCHAR, BIKE_COLOR VARCHAR
) COMMENT = 'Tabla para copiar los archivos sin modificar desde nuestro stage Bikes.';

CREATE OR REPLACE TABLE RAW_STATIONS (
    STATION_ID VARCHAR, CAPACITY VARCHAR, LATITUDE VARCHAR, LONGITUDE VARCHAR, STATION_NAME VARCHAR
) COMMENT = 'Tabla para copiar los archivos sin modificar desde nuestro stage Bikes.';

-- Luego de tener las tablas preparadas, realizaremos los COPY_INTO para traer los datos desde nuestro Stage.
-- Traemos a la tabla RAW_JOURNEYS
COPY INTO RAW_JOURNEYS
    FROM (
        SELECT
            t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12, t.$13, t.$14, t.$15
        FROM @JOURNEYS_CSV_STAGE t
    )
    FILE_FORMAT = FF_CSV_JOURNEYS
    ON_ERROR = 'CONTINUE';

-- Traemos a la tabla RAW_WEATHER
COPY INTO RAW_WEATHER
    FROM (
        SELECT
            t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12
        FROM @WEATHER_CSV_STAGE t
    )
    FILE_FORMAT = FF_CSV_WEATHER
    ON_ERROR = 'CONTINUE';

-- Confirmamos la carga de datos y mostramos algunos querys
SELECT COUNT(*) FROM RAW_JOURNEYS; --1048575
SELECT COUNT(*) FROM RAW_WEATHER; --10886

-- Ver algunas filas cargadas
SELECT * FROM RAW_JOURNEYS LIMIT 10; --Correcto
SELECT * FROM RAW_WEATHER LIMIT 10; --Correcto

-- Aquí comenzaremos las consultas para inspeccionar el contenido de los archivos directamente desde los stages.

-- Nos posicionamos en el WH correspondiente
USE WAREHOUSE CYCLE_WORLD_WH;

-- Inspeccionamos el Stage Journeys.csv
-- Intentamos leer las primeras 10 filas sin cargarlas aún en nuestro proyecto
SELECT $1, $2, $3
FROM @CYCLE_WORLD_DB.RAW.JOURNEYS_CSV_STAGE/Journeys.csv
LIMIT 10;

-- El resultado es una sola columna con información, con las columnas separadas por ';'
-- Los nombres de las columnas deben ser: Journey Duration;Journey ID;End Date;End Month;End Year;End Hour;End Minute;End Station ID;Start Date;Start Month;Start Year;Start Hour;Start Minute;Start Station ID;Bike ID
-- Generamos un File_Format para poder entender mejor los datos

-- File Format para Journeys.csv 
CREATE OR REPLACE FILE FORMAT FF_CSV_Journeys
    TYPE = 'CSV'                     -- Tipo de archivo
    FIELD_DELIMITER = ';'            -- Delimitador de campos
    SKIP_HEADER = 1                  -- Saltar la primera fila (encabezado)
    EMPTY_FIELD_AS_NULL = TRUE       -- Tratar campos vacíos como NULL
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE -- Más flexible si alguna fila tiene más/menos delimitadores
    COMMENT = 'File Format para el archivo JOURNEYS.CSV';

--Con el File_Format, utilizamos la función INFER_SCHEMA de Snowflake para detectar la información de las columnas

SELECT *
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@CYCLE_WORLD_DB.RAW.JOURNEYS_CSV_STAGE', -- El stage donde está el archivo
            FILES => 'Journeys.csv',                     -- El archivo específico a analizar
            FILE_FORMAT => 'FF_CSV_Journeys'             -- ¡Importante! Usamos nuestro formato
        )
);

--El resultado nos confirma lo que podíamos leer previamente, hay 15 columnas, todas de tipo numérico "c15	NUMBER(5, 0)	TRUE	$15::NUMBER(5, 0)	Journeys.csv	14"
 
--Ahora que sabemos que nuestro File_Format funciona, veremos los datos en su respectiva tabla manteniendo los nombres de su columna original
SELECT
    $1 AS Journey_Duration,
    $2 AS Journey_ID,
    $3 AS End_Date,
    $4 AS End_Month,
    $5 AS End_Year,
    $6 AS End_Hour,
    $7 AS End_Minute,
    $8 AS End_Station_ID,
    $9 AS Start_Date,
    $10 AS Start_Month,
    $11 AS Start_Year,
    $12 AS Start_Hour,
    $13 AS Start_Minute,
    $14 AS Start_Station_ID,
    $15 AS Bike_ID
FROM
    @CYCLE_WORLD_DB.RAW.JOURNEYS_CSV_STAGE/Journeys.csv 
    (FILE_FORMAT => 'FF_CSV_Journeys')
LIMIT 10;

-- Perfecto, podemos continuar con nuestro siguiente archivo. 
-- Inspeccionamos el Stage Weather.csv
-- Intentamos leer las primeras 5 filas sin cargarlas aún en nuestro proyecto
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
FROM
    @CYCLE_WORLD_DB.RAW.WEATHER_CSV_STAGE/Weather.csv -- Nombre exacto del archivo
LIMIT 15;

-- En este archivo, vemos que la información está correctamente en sus respectivas columnas
-- Solo hay que corregir la primera fila "datetime	season	holiday	workingday	weather	temp	atemp	humidity	windspeed	casual	registered	count"
CREATE OR REPLACE FILE FORMAT FF_CSV_Weather
    TYPE = 'CSV'
    FIELD_DELIMITER = ','            
    SKIP_HEADER = 1                  
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    COMMENT = 'File Format para el archivo Weather.csv';

-- En este caso, la cantidad de columnas es mucho más clara, por lo cual no necesitamos la función Infer_Schema
SELECT
    $1 AS datetime,
    $2 AS season,
    $3 AS holiday,
    $4 AS workingday,
    $5 AS weather,
    $6 AS temp,
    $7 AS atemp,
    $8 AS humidity,
    $9 AS windspeed,
    $10 AS casual,
    $11 AS registered,
    $12 AS count
FROM
    @CYCLE_WORLD_DB.RAW.WEATHER_CSV_STAGE/Weather.csv -- Nombre exacto del archivo
    (FILE_FORMAT => 'FF_CSV_weather') -- Aplicar el formato
LIMIT 10;

-- El último archivo será trabajado en un entorno de Python al ser un archivo .XSLX

-- Continuamos con los archivos de nuestro stage
-- En este caso, los datos se encontraban en un libro en formato .XLXS, separados en dos hojas
-- Para solucionarlo, primero fueron convertidos a .CSV por separado, y luego cargados en el stage.

-- Usar el WH
USE WAREHOUSE CYCLE_WORLD_WH;

SELECT
    $1, $2, $3, $4, $5, $6
FROM
    @CYCLE_WORLD_DB.RAW.BIKES_XLSX_STAGE/Stations.csv
LIMIT 5;

-- Como se puede analizar, la primera fila tiene los nombres de las columnas lo demás parece correcto
-- "Station ID	Capacity	Latitude	Longitude	Station Name	"
-- Probamos con el File_Format creado anteriormente

SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@CYCLE_WORLD_DB.RAW.BIKES_XLSX_STAGE/Stations.csv',
        FILE_FORMAT => 'FF_CSV_Weather' -- Usar el formato de coma
    )
);

-- Se puede observar que el archivo Stations tiene 7 columnas, 4 en formato numérico y 3 en formato texto
-- "c7	TEXT	TRUE	$7::TEXT	Stations.csv	6"
-- Realizamos ahora un ajuste al File_Format y la verificación

SELECT
    $1 AS STATION_ID,
    $2 AS CAPACITY,
    $3 AS LATITUDE,
    $4 AS LONGITUDE,
    $5 AS STATION_NAME,
FROM
        @CYCLE_WORLD_DB.RAW.BIKES_XLSX_STAGE/Stations.csv
    (FILE_FORMAT => 'FF_CSV_WEATHER') 
LIMIT 15;

-- Perfecto, ahora cargaremos los datos en la tabla anteriormente creada
COPY INTO CYCLE_WORLD_DB.RAW.RAW_STATIONS
    FROM (
        SELECT
            s.$1, s.$2, s.$3, s.$4, s.$5
        FROM @CYCLE_WORLD_DB.RAW.BIKES_XLSX_STAGE/Stations.csv s
    )
    FILE_FORMAT = FF_CSV_WEATHER
    ON_ERROR = 'CONTINUE';
    
-- Verificamos la carga
SELECT *
FROM RAW_STATIONS
LIMIT 10;

-- Todo en orden, procedemos con el último archivo Bikes.csv
SELECT
    $1, $2, $3
FROM
    @CYCLE_WORLD_DB.RAW.BIKES_XLSX_STAGE/Bikes.csv 
LIMIT 5;

-- Confirmamos que el número de sea 3 columnas
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@CYCLE_WORLD_DB.RAW.BIKES_XLSX_STAGE/Bikes.csv',
        FILE_FORMAT => 'FF_CSV_WEATHER' 
    )
);

-- Probamos los nombres de las columnas antes de cargar

SELECT
    $1 AS BIKE_ID,
    $2 AS BIKE_MODEL,
    $3 AS BIKE_COLOR
FROM
    @CYCLE_WORLD_DB.RAW.BIKES_XLSX_STAGE/Bikes.csv
    (FILE_FORMAT => 'FF_CSV_WEATHER') -- Aplicar el formato
LIMIT 5;

-- Por último, llevamos la información a nuestra tabla previamente crada
COPY INTO CYCLE_WORLD_DB.RAW.RAW_BIKES
    FROM (
        SELECT
            b.$1, b.$2, b.$3
        FROM @CYCLE_WORLD_DB.RAW.BIKES_XLSX_STAGE/Bikes.csv b
    )
    FILE_FORMAT = FF_CSV_WEATHER
    ON_ERROR = 'CONTINUE';

-- Y confirmamos los datos cargados
SELECT * FROM RAW_BIKES LIMIT 10;

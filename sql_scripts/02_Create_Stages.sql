
-- Aquí se crearán los stages donde se cargarán los archivos.

-- Usamos el schema Raw
USE SCHEMA CYCLE_WORLD_DB.RAW;

-- Creamos el stage para el archivo Journeys.csv
CREATE STAGE JOURNEYS_CSV_STAGE
    COMMENT = 'Stage interno para el archivo Journeys.csv';

-- Creamos el stage para el archivo Weather.csv
CREATE STAGE WEATHER_CSV_STAGE
    COMMENT = 'Stage interno para el archivo Weather.csv';

-- Creamos el stage para el archivo Stations_-_Bikes.xlsx
CREATE STAGE BIKES_XLSX_STAGE
    COMMENT = 'Stage interno para el archivo Stations_-_Bikes.xlsx';

-- Verificamos la creación
SHOW STAGES IN SCHEMA CYCLE_WORLD_DB.RAW;

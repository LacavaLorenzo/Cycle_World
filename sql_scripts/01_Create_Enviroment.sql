-- Aquí se creará la base de datos, esquemas y warehouse a utilizar para el cliente Cycle World.

-- Creamos el Warehouse que se utilizará para el cliente
CREATE WAREHOUSE CYCLE_WORLD_WH
    WAREHOUSE_SIZE = 'X-SMALL' 
    AUTO_SUSPEND = 60          
    AUTO_RESUME = TRUE           
    INITIALLY_SUSPENDED = TRUE   
    COMMENT = 'Warehouse exclusivo de Cycle World.';

-- Nos posicionamos en el WH creado 
USE WAREHOUSE CYCLE_WORLD_WH;

-- Creamos un nuevo DataBase 
CREATE OR REPLACE DATABASE CYCLE_WORLD_DB
    COMMENT = 'Base de datos para el cliente Cycle World';

-- Nos posicionamos en la DB creada
USE DATABASE CYCLE_WORLD_DB;

-- Creamos un primer esquema llamado RAW para ubicar los datos crudos 
CREATE OR REPLACE SCHEMA RAW
    COMMENT = 'Schema para almacenar los archivos fuente sin transformar.';

-- Creamos un segundo esquema PROCESSED para los datos ya transformados
CREATE OR REPLACE SCHEMA PROCESSED
    COMMENT = 'Schema para almacenar los datos limpios y transformados.';

-- Creamos un tercer esquema ANALYTICS para las vistas
CREATE OR REPLACE SCHEMA ANALYTICS
    COMMENT = 'Schema para las vistas finales.';

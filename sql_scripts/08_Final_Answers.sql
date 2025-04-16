-- Aquí presentamos las consultas finales para responder a los requerimientos
-- del cliente, utilizando las Vistas creadas en el schema ANALYTICS.

-- Establecer el contexto
USE SCHEMA CYCLE_WORLD_DB.ANALYTICS; 
USE WAREHOUSE CYCLE_WORLD_WH;

-- ----------------------------------------------------------------------------
-- Requerimiento #1: Reporte Resumen Simple
-- ----------------------------------------------------------------------------
SELECT * FROM CYCLE_WORLD_DB.ANALYTICS.JOURNEYS_TO_STATIONS_VIEW
ORDER BY FECHA_INICIO -- Ordenar por fecha
LIMIT 20; -- Limitar a conveniencia

-- ----------------------------------------------------------------------------
-- Requerimiento #2: Reporte de Actividad por Estación (General)
-- ----------------------------------------------------------------------------
SELECT * FROM CYCLE_WORLD_DB.ANALYTICS.STATION_ACTIVITY
ORDER BY STATION_NAME, ZONA_HORARIA, ACTIVITY_TYPE, BIKE_COLOR -- Ordenar
LIMIT 50; -- Limitar a conveniencia

-- ----------------------------------------------------------------------------
-- Requerimiento #3: Reporte de Actividad por Estación (Marylebone)
-- ----------------------------------------------------------------------------
SELECT * FROM CYCLE_WORLD_DB.ANALYTICS.STATION_ACTIVITY_MARYLEBONE
ORDER BY STATION_NAME, ZONA_HORARIA, ACTIVITY_TYPE, BIKE_COLOR -- Ordenar
LIMIT 50; -- Limitar a conveniencia

-- ----------------------------------------------------------------------------
-- Pregunta #4: Top 10 Estaciones Más Concurridas
-- ----------------------------------------------------------------------------

SELECT * FROM CYCLE_WORLD_DB.ANALYTICS.TOP_TEN_STATIONS; -- La vista ya está ordenada y limitada a 10

-- ----------------------------------------------------------------------------
-- Las 10 Estaciones Más Concurridas son:
--Hyde Park Corner
--Belgrove Street
--Albert Gate
--Waterloo Station 3
--Black Lion Gate
--Triangle Car Park
--Hop Exchange
--Aquatic Centre
--Storey's Gate
--Brushfield Street
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- Pregunta #5: Porcentaje Viajes Lluviosos
-- ----------------------------------------------------------------------------
SELECT * FROM CYCLE_WORLD_DB.ANALYTICS.RAINY_PERCENTAGE;
-- ----------------------------------------------------------------------------
-- El porcentaje de viajes bajo la lluvia es 9,17%
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- Pregunta #6: Duración Promedio Días Despejados (en minutos)
-- ----------------------------------------------------------------------------
SELECT * FROM CYCLE_WORLD_DB.ANALYTICS.AVG_JOURNEY_SUNNY;
-- ----------------------------------------------------------------------------
-- La duración promedio de vijaes en días soleados es de 20 minutos
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- Pregunta #7: Uso Color Bicicleta (Más/Menos Usado)
-- ----------------------------------------------------------------------------
SELECT * FROM CYCLE_WORLD_DB.ANALYTICS.BIKE_COLOR; 
-- ----------------------------------------------------------------------------
-- La bicicleta más usada es la Amarilla (Yellow) y la menos usada es la Azul (Blue)
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- Pregunta #8: Estaciones Sin Bicicletas Disponibles
-- ----------------------------------------------------------------------------
-- Esta pregunta no se puede responder de forma fiable debido a inconsistencias graves en los datos fuente (ej: salidas que exceden la capacidad).
SELECT 
    'Pregunta #8 no respondible fiablemente' AS Status, 
    'Datos de salidas inconsistentes en la fuente original (ej: 181 bicis distintas saliendo de Estación 14 / Capacidad 48 en una hora).' AS Razon;

-- ----------------------------------------------------------------------------

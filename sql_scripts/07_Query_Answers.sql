-- En este worksheet se responderán los querys que pidió el cliente Cycle_World

-- Entregar un reporte resumen simple con los siguientes datos:
-- Numero de viaje
-- Fecha de inicio y fecha de fin en formato ‘dd/mm/yyyy’
-- Nombre de estación
-- Sector en el que se ubica la estación
-- Color de la bicicleta utilizada


-- Definir el contexto 
USE SCHEMA CYCLE_WORLD_DB.PROCESSED; 
USE WAREHOUSE CYCLE_WORLD_WH; 

CREATE OR REPLACE VIEW CYCLE_WORLD_DB.ANALYTICS.JOURNEYS_TO_STATIONS_VIEW 
AS-- Consulta principal
    SELECT
        j.JOURNEY_ID,   
        -- Tomamos el nombre y sector de la estación donde empieza desde FACT_STATIONS
        s.STATION_NAME AS NOMBRE_ESTACION_INICIO,
        s.SECTOR AS SECTOR_ESTACION_INICIO,
        -- Formateamos las fechas DD/MM/YYYY
        TO_VARCHAR(j.START_TIMESTAMP, 'DD/MM/YYYY') AS FECHA_INICIO,     
        TO_VARCHAR(j.END_TIMESTAMP, 'DD/MM/YYYY') AS FECHA_FIN,
        -- Tomamos el nombre y sector de la estación donde termina desde FACT_STATIONS
        s_end.STATION_NAME AS NOMBRE_ESTACION_FIN,
        s_end.SECTOR AS SECTOR_ESTACION_FIN,   
        -- El color de la bicicleta desde FACT_BIKES
        b.BIKE_COLOR AS COLOR_BICICLETA,
        -- Y el ID de la bicicleta, para confirmar que sean viajes distintos
        j.BIKE_ID AS BIKE_ID
    FROM
        -- Tomamos como tabla principal la de los viajes
        CYCLE_WORLD_DB.PROCESSED.FACT_JOURNEYS AS j    
    LEFT JOIN 
        -- Unimos según el ID de la estación
        CYCLE_WORLD_DB.PROCESSED.FACT_STATIONS AS s 
        ON j.START_STATION_ID = s.STATION_ID 
        -- Usamos LEFT JOIN por si algún ID de estación en FACT_JOURNEYS no existiera, para no perder el registro del viaje.    
    LEFT JOIN 
        -- Unimos con la información de bicicletas usando el ID de la bicicleta
        CYCLE_WORLD_DB.PROCESSED.FACT_BIKES AS b 
        ON j.BIKE_ID = b.BIKE_ID
        -- Usamos LEFT JOIN por si algún ID de bicicleta no existiera en FACT_BIKES.
    LEFT JOIN 
        CYCLE_WORLD_DB.PROCESSED.FACT_STATIONS AS s_end -- Alias diferente
        ON j.END_STATION_ID = s_end.STATION_ID -- Unir por estación de FIN
    ORDER BY -- Ordenamos según fecha
        j.JOURNEY_ID;

-- Continuamos con el reporte:

CREATE OR REPLACE VIEW CYCLE_WORLD_DB.ANALYTICS.STATION_ACTIVITY AS
    WITH StationActivity AS (
        -- Salidas (Departures)
        SELECT 
            START_STATION_ID AS STATION_ID,
            START_TIMESTAMP AS ACTIVITY_TIMESTAMP,
            BIKE_ID,
            'Departure' AS ACTIVITY_TYPE -- Marcamos como Salida
        FROM 
            CYCLE_WORLD_DB.PROCESSED.FACT_JOURNEYS
        WHERE 
            START_STATION_ID IS NOT NULL AND START_TIMESTAMP IS NOT NULL -- Asegurar datos válidos
    
        UNION ALL -- Une los dos conjuntos
    
        -- Llegadas (Arrivals)
        SELECT 
            END_STATION_ID AS STATION_ID,
            END_TIMESTAMP AS ACTIVITY_TIMESTAMP,
            BIKE_ID,
            'Arrival' AS ACTIVITY_TYPE -- Marcamos como Llegada
        FROM 
            CYCLE_WORLD_DB.PROCESSED.FACT_JOURNEYS
        WHERE 
            END_STATION_ID IS NOT NULL AND END_TIMESTAMP IS NOT NULL -- Asegurar datos válidos
    )
    -- Consulta principal que procesa la actividad unificada
    SELECT
        s.STATION_ID,
        s.STATION_NAME,
        s.SECTOR,
        -- Determinar la Zona Horaria basada en la hora de la actividad
        CASE 
            WHEN HOUR(sa.ACTIVITY_TIMESTAMP) >= 7 AND HOUR(sa.ACTIVITY_TIMESTAMP) <= 9 THEN 'Mañana (07-09)'
            WHEN HOUR(sa.ACTIVITY_TIMESTAMP) >= 10 AND HOUR(sa.ACTIVITY_TIMESTAMP) <= 16 THEN 'Valle (10-16)'
            WHEN HOUR(sa.ACTIVITY_TIMESTAMP) >= 17 AND HOUR(sa.ACTIVITY_TIMESTAMP) <= 23 THEN 'Tarde (17-23)'
            ELSE 'Noche/Madrugada (00-06)' -- Opcional: Agrupar horas fuera de los rangos pedidos
        END AS ZONA_HORARIA,
        b.BIKE_COLOR,
        sa.ACTIVITY_TYPE, -- 'Arrival' o 'Departure'
        COUNT(*) AS NUMERO_BICICLETAS -- Contar el número de eventos para cada grupo
    
    FROM
        StationActivity AS sa -- Nuestro CTE con todos los eventos
    LEFT JOIN
        CYCLE_WORLD_DB.PROCESSED.FACT_STATIONS AS s ON sa.STATION_ID = s.STATION_ID
    LEFT JOIN
        CYCLE_WORLD_DB.PROCESSED.FACT_BIKES AS b ON sa.BIKE_ID = b.BIKE_ID
    
    GROUP BY 
        -- Agrupar por todas las dimensiones que queremos en el reporte
        s.STATION_ID,
        s.STATION_NAME,
        s.SECTOR,
        ZONA_HORARIA, -- Agrupar por la zona horaria calculada
        b.BIKE_COLOR,
        sa.ACTIVITY_TYPE
    
    ORDER BY
        -- Ordenar para facilitar la lectura
        s.STATION_NAME,
        s.SECTOR,
        ZONA_HORARIA,
        b.BIKE_COLOR,
        sa.ACTIVITY_TYPE
    ;

CREATE OR REPLACE VIEW CYCLE_WORLD_DB.ANALYTICS.STATION_ACTIVITY_MARYLEBONE AS
    WITH StationActivity AS (
        -- Salidas (Departures)
        SELECT 
            START_STATION_ID AS STATION_ID,
            START_TIMESTAMP AS ACTIVITY_TIMESTAMP,
            BIKE_ID,
            'Departure' AS ACTIVITY_TYPE -- Marcamos como Salida
        FROM 
            CYCLE_WORLD_DB.PROCESSED.FACT_JOURNEYS
        WHERE 
            START_STATION_ID IS NOT NULL AND START_TIMESTAMP IS NOT NULL -- Asegurar datos válidos
    
        UNION ALL -- Une los dos conjuntos
    
        -- Llegadas (Arrivals)
        SELECT 
            END_STATION_ID AS STATION_ID,
            END_TIMESTAMP AS ACTIVITY_TIMESTAMP,
            BIKE_ID,
            'Arrival' AS ACTIVITY_TYPE -- Marcamos como Llegada
        FROM 
            CYCLE_WORLD_DB.PROCESSED.FACT_JOURNEYS
        WHERE 
            END_STATION_ID IS NOT NULL AND END_TIMESTAMP IS NOT NULL -- Asegurar datos válidos
    )
    -- Consulta principal que procesa la actividad unificada
    SELECT
        s.STATION_ID,
        s.STATION_NAME,
        s.SECTOR,
        -- Determinar la Zona Horaria basada en la hora de la actividad
        CASE 
            WHEN HOUR(sa.ACTIVITY_TIMESTAMP) >= 7 AND HOUR(sa.ACTIVITY_TIMESTAMP) <= 9 THEN 'Mañana (07-09)'
            WHEN HOUR(sa.ACTIVITY_TIMESTAMP) >= 10 AND HOUR(sa.ACTIVITY_TIMESTAMP) <= 16 THEN 'Valle (10-16)'
            WHEN HOUR(sa.ACTIVITY_TIMESTAMP) >= 17 AND HOUR(sa.ACTIVITY_TIMESTAMP) <= 23 THEN 'Tarde (17-23)'
            ELSE 'Noche/Madrugada (00-06)' -- Opcional: Agrupar horas fuera de los rangos pedidos
        END AS ZONA_HORARIA,
        b.BIKE_COLOR,
        sa.ACTIVITY_TYPE, -- 'Arrival' o 'Departure'
        COUNT(*) AS NUMERO_BICICLETAS -- Contar el número de eventos para cada grupo
    
    FROM
        StationActivity AS sa -- Nuestro CTE con todos los eventos
    LEFT JOIN
        CYCLE_WORLD_DB.PROCESSED.FACT_STATIONS AS s ON sa.STATION_ID = s.STATION_ID
    LEFT JOIN
        CYCLE_WORLD_DB.PROCESSED.FACT_BIKES AS b ON sa.BIKE_ID = b.BIKE_ID
    
    WHERE s.SECTOR = 'Marylebone'
    GROUP BY 
        -- Agrupar por todas las dimensiones que queremos en el reporte
        s.STATION_ID,
        s.STATION_NAME,
        s.SECTOR,
        ZONA_HORARIA, -- Agrupar por la zona horaria calculada
        b.BIKE_COLOR,
        sa.ACTIVITY_TYPE
    
    ORDER BY
        -- Ordenar para facilitar la lectura
        s.STATION_NAME,
        s.SECTOR,
        ZONA_HORARIA,
        b.BIKE_COLOR,
        sa.ACTIVITY_TYPE
;


CREATE OR REPLACE VIEW CYCLE_WORLD_DB.ANALYTICS.TOP_TEN_STATIONS AS
    WITH StationActivityTotal AS (
        SELECT START_STATION_ID AS STATION_ID FROM CYCLE_WORLD_DB.PROCESSED.FACT_JOURNEYS WHERE START_STATION_ID IS NOT NULL
        UNION ALL
        SELECT END_STATION_ID AS STATION_ID FROM CYCLE_WORLD_DB.PROCESSED.FACT_JOURNEYS WHERE END_STATION_ID IS NOT NULL
    )
    -- Consulta principal
    SELECT 
        s.STATION_ID,
        s.STATION_NAME,
        s.SECTOR,
        COUNT(*) AS TOTAL_ACTIVITY -- Contar todas las filas (llegadas + salidas) por estación
    FROM 
        StationActivityTotal AS sat
    JOIN 
       CYCLE_WORLD_DB.PROCESSED.FACT_STATIONS AS s ON sat.STATION_ID = s.STATION_ID
    GROUP BY 
        s.STATION_ID, s.STATION_NAME, s.SECTOR
    ORDER BY 
        TOTAL_ACTIVITY DESC -- Ordenar de mayor a menor concurrencia
    LIMIT 10; -- Limitar a las 10 primeras

-- Las 10 estaciones más concurridas son: Hyde Park Corner, Belgrove Street, Albert Gate, Waterloo Station 3, Black Lion Gate
-- Triangle Car Park, Hop Exchange, Aquatic Centre, Storey's Gate, Brushfield Street

CREATE OR REPLACE VIEW CYCLE_WORLD_DB.ANALYTICS.RAINY_PERCENTAGE AS
    SELECT
        -- Contar todos los viajes que tienen una hora de inicio válida
        COUNT(j.TRIP_SK) AS TOTAL_JOURNEYS,
    
        -- Contar solo los viajes donde el código de clima unido es 3 o 4
        COUNT_IF(w.WEATHER_CODE IN (3, 4)) AS RAINY_JOURNEYS,
    
        -- Calcular el porcentaje. Multiplicamos por 100.0 para asegurar división decimal.
        -- Usamos DIV0NULL para evitar error si TOTAL_JOURNEYS fuera 0 (poco probable).
        DIV0NULL(RAINY_JOURNEYS * 100.0, TOTAL_JOURNEYS) AS PERCENTAGE_RAINY
    
    FROM
        -- Empezamos desde los viajes
        CYCLE_WORLD_DB.PROCESSED.FACT_JOURNEYS AS j
    LEFT JOIN
        -- Unimos con el clima usando la FECHA y HORA de inicio del viaje
        CYCLE_WORLD_DB.PROCESSED.FACT_WEATHER AS w
        ON DATE(j.START_TIMESTAMP) = w.WEATHER_DATE -- Unir por fecha
       AND HOUR(j.START_TIMESTAMP) = w.WEATHER_HOUR
    ; -- Unir por hora

-- No se necesita WHERE aquí, ya que queremos el porcentaje sobre el total de viajes.
-- Si hubiera viajes con START_TIMESTAMP nulo, no se unirían ni contarían (lo cual es correcto).
-- Usamos LEFT JOIN por si faltara algún dato de clima, aunque no debería impactar mucho el % total.


-- Con un total de 1.048.575 viajes, solo 96.208 fueron en horarios lluviosos. Para un total del %9,17
CREATE OR REPLACE VIEW CYCLE_WORLD_DB.ANALYTICS.AVG_JOURNEY_SUNNY AS
    SELECT
        -- Calcular el promedio de la columna de duración en minutos
        AVG(j.JOURNEY_DURATION_MINUTES) AS AVG_DURATION_MINUTES_CLEAR_DAYS
    
    FROM
        -- Empezamos desde los viajes
        CYCLE_WORLD_DB.PROCESSED.FACT_JOURNEYS AS j
    INNER JOIN 
        -- Unimos con el clima por fecha y hora de inicio.
        -- INNER JOIN es adecuado aquí porque solo nos interesan los viajes
        -- que tienen un registro de clima correspondiente Y que ese clima sea despejado.
        CYCLE_WORLD_DB.PROCESSED.FACT_WEATHER AS w 
        ON DATE(j.START_TIMESTAMP) = w.WEATHER_DATE 
       AND HOUR(j.START_TIMESTAMP) = w.WEATHER_HOUR
    
    WHERE
        -- Filtramos para incluir solo los registros donde el clima fue despejado
        w.WEATHER_CODE = 1;

-- En los días soleados, la duración promedio en minutos de un viaje es de 20 minutos.

CREATE OR REPLACE VIEW CYCLE_WORLD_DB.ANALYTICS.BIKE_COLOR AS
    SELECT
        b.BIKE_COLOR,
        COUNT(*) AS JOURNEY_COUNT -- Contar el número de viajes para este color
    
    FROM
        -- Empezamos desde los viajes
        CYCLE_WORLD_DB.PROCESSED.FACT_JOURNEYS AS j
    INNER JOIN
        -- Unimos con la dimensión de bicicletas para obtener el color
        CYCLE_WORLD_DB.PROCESSED.FACT_BIKES AS b 
        ON j.BIKE_ID = b.BIKE_ID
        -- INNER JOIN es adecuado: solo contamos viajes de bicis cuyo color conocemos.
    
    -- WHERE b.BIKE_COLOR IS NOT NULL -- Opcional: Excluir si hubiera colores nulos
    
    GROUP BY
        b.BIKE_COLOR -- Agrupar por color
    
    ORDER BY
        JOURNEY_COUNT DESC;

-- La bici amarilla es la más usada, con un total de 212.210 viajes
-- La bici azul es la menos usada, con un total de 205.153 viajes

-- Intentamos responder la última consulta

-- 1. CTE para obtener el primer día de datos presente en los viajes
WITH FirstDay AS (
    SELECT MIN(DATE(START_TIMESTAMP)) as first_data_date 
    FROM FACT_JOURNEYS
), 
-- 2. CTE para identificar las estaciones de Marylebone y su capacidad
MaryleboneStations AS (
    SELECT STATION_ID, STATION_NAME, CAPACITY, SECTOR 
    FROM fact_STATIONS 
    WHERE SECTOR = 'Marylebone' -- Filtrar por el sector deseado
),
-- 3. CTE para obtener todos los eventos (llegadas +1, salidas -1) por hora para esas estaciones en ese primer día
HourlyActivityMarylebone AS (
    SELECT 
        ms.STATION_ID, ms.CAPACITY,
        HOUR(j.START_TIMESTAMP) AS ACTIVITY_HOUR,
        -1 AS BIKE_CHANGE -- Salida resta 1
    FROM FACT_JOURNEYS AS j
    JOIN MaryleboneStations AS ms ON j.START_STATION_ID = ms.STATION_ID
    WHERE DATE(j.START_TIMESTAMP) = (SELECT first_data_date FROM FirstDay)
      AND j.START_TIMESTAMP IS NOT NULL

    UNION ALL

    SELECT 
        ms.STATION_ID, ms.CAPACITY,
        HOUR(j.END_TIMESTAMP) AS ACTIVITY_HOUR,
        1 AS BIKE_CHANGE -- Llegada suma 1
    FROM FACT_JOURNEYS AS j
    JOIN MaryleboneStations AS ms ON j.END_STATION_ID = ms.STATION_ID
    WHERE DATE(j.END_TIMESTAMP) = (SELECT first_data_date FROM FirstDay)
      AND j.END_TIMESTAMP IS NOT NULL
),
-- 4. CTE para calcular el cambio neto por hora/estación (agrupando los eventos de la misma hora)
HourlyNetChangeMarylebone AS (
    SELECT 
        STATION_ID, 
        CAPACITY,
        ACTIVITY_HOUR, 
        SUM(BIKE_CHANGE) as HOURLY_NET_CHANGE
    FROM HourlyActivityMarylebone
    GROUP BY STATION_ID, CAPACITY, ACTIVITY_HOUR
),
-- 5. CTE para calcular el balance acumulado y el inventario simulado
RunningBalanceSimulation AS (
    SELECT
        hnc.STATION_ID, 
        hnc.CAPACITY, 
        hnc.ACTIVITY_HOUR, 
        hnc.HOURLY_NET_CHANGE,
        -- Calcular el cambio acumulado desde el inicio del día (hora 0 implícita) hasta la hora actual
        SUM(hnc.HOURLY_NET_CHANGE) OVER (
            PARTITION BY hnc.STATION_ID 
            ORDER BY hnc.ACTIVITY_HOUR 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as CUMULATIVE_CHANGE,
        -- Calcular inventario: Capacidad inicial (supuesto) + cambio acumulado
        hnc.CAPACITY + CUMULATIVE_CHANGE AS SIMULATED_BALANCE_UNCAPPED,
        -- Ajustar inventario a los límites [0, Capacidad]
        GREATEST(0, -- Límite inferior 0
            LEAST(hnc.CAPACITY, SIMULATED_BALANCE_UNCAPPED) -- Límite superior = Capacidad
        ) as SIMULATED_INVENTORY
    FROM HourlyNetChangeMarylebone hnc
)
-- Consulta final: Mostrar las horas donde el inventario simulado llegó a 0 o menos (antes del ajuste a 0)
SELECT 
    (SELECT first_data_date FROM FirstDay) AS ACTIVITY_DATE, -- Añadimos la fecha para contexto
    rbs.ACTIVITY_HOUR,
    rbs.STATION_ID, 
    ms.STATION_NAME, 
    rbs.CAPACITY, 
    rbs.HOURLY_NET_CHANGE,
    rbs.CUMULATIVE_CHANGE,
    rbs.SIMULATED_BALANCE_UNCAPPED, -- Mostrar el balance antes de ajustar a 0/Capacidad
    rbs.SIMULATED_INVENTORY -- Mostrar el inventario final ajustado
FROM 
    RunningBalanceSimulation rbs
JOIN 
    MaryleboneStations ms ON rbs.STATION_ID = ms.STATION_ID
WHERE 
    rbs.SIMULATED_INVENTORY <= 0 -- Nos interesan los momentos donde llegó a 0 (o teóricamente menos)
ORDER BY 
    rbs.STATION_ID, 
    rbs.ACTIVITY_HOUR;

--No se puede responder por incongruencias en los datos

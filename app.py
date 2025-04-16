# --- Importar Librerías ---
import streamlit as st
import snowflake.connector # Usar el conector estándar
import pandas as pd
# Ya no necesitamos importar get_active_session

# --- Función de Conexión a Snowflake (Usando st.secrets) ---
@st.cache_resource # Cachear el recurso de conexión
def init_connection():
    """Inicializa la conexión a Snowflake usando credenciales de st.secrets."""
    try:
        conn = snowflake.connector.connect(
            **st.secrets["snowflake"], # Lee user, password, account, etc. desde secrets.toml
            client_session_keep_alive=True 
        )
        return conn
    except Exception as e:
        st.error(f"Error al conectar a Snowflake: {e}")
        st.stop() # Detener la app si la conexión falla

# --- Función para Ejecutar Consultas (Usando el Conector) ---
@st.cache_data(ttl=600) # Cachear datos por 10 minutos
def run_query(query: str) -> pd.DataFrame:
    """Ejecuta una consulta en Snowflake usando el conector y devuelve un DataFrame de Pandas."""
    try:
        conn = init_connection() # Obtiene la conexión cacheada
        # st.write(f"Ejecutando consulta: {query[:100]}...") # Descomentar para depurar
        with conn.cursor() as cur:
            cur.execute(query)
            df = cur.fetch_pandas_all() # Trae todos los resultados a Pandas
        # st.write("Consulta completada.") # Descomentar para depurar
        return df
    except Exception as e:
        st.error(f"Error al ejecutar la consulta: {e}")
        return pd.DataFrame() # Devolver DF vacío en caso de error

# --- Configuración de la Página Streamlit (sin cambios) ---
st.set_page_config(
    page_title="Cycle World Analytics",
    page_icon="🚲", 
    layout="wide" 
)

# --- Barra Lateral de Navegación (sin cambios) ---
st.sidebar.title("Navegación 🧭") 
page_options = ["Reportes Tabulares 📄", "Dashboard Analítico 📊"] 
page = st.sidebar.radio(
    "Selecciona la Página:",
    options=page_options,
    key="navigation" 
)
st.sidebar.markdown("---") 

# --- Contenido Principal (Depende de la página seleccionada) ---

# --- Página 1: Reportes Tabulares (Usa run_query) ---
if page == page_options[0]: 
    
    st.title("🚲 Cycle World")
    st.subheader("Reporte de funcionamiento de las estaciones en Londres")
    st.markdown("---") 

    limit_options = [100, 200, 300, 400, 500] # Opciones fijas (se podrían quitar si se prefiere solo number_input)

    # --- Reporte 1: Resumen Simple ---
    st.markdown("**1. Reporte de viajes simple**")
    limit1_input = st.number_input(
        label="Número de filas a mostrar:", min_value=1, max_value=10000, value=100, step=50,
        key='limit_reporte_1_num', help="Introduce el número de filas que deseas visualizar (máx 10000)."
    )
    limit1 = int(limit1_input) 
    query1 = f"SELECT * FROM CYCLE_WORLD_DB.ANALYTICS.JOURNEYS_TO_STATIONS_VIEW ORDER BY FECHA_INICIO LIMIT {limit1};"
    # *** Cambio: Usar run_query en lugar de run_query_sis ***
    df_reporte1 = run_query(query1) 
    if not df_reporte1.empty: st.dataframe(df_reporte1, use_container_width=True)
    elif limit1 > 0 : st.warning("No se pudieron cargar los datos para el Reporte 1 o la vista está vacía.")
    st.markdown("---") 

    # --- Reporte 2: Actividad por Estación (General) ---
    st.markdown("**2. Reporte de actividades por estación**")
    limit2_input = st.number_input(
        label="Número de filas a mostrar:", min_value=1, max_value=10000, value=100, step=50,
        key='limit_reporte_2_num', help="Introduce el número de filas que deseas visualizar (máx 10000)."
    )
    limit2 = int(limit2_input)
    query2 = f"SELECT * FROM CYCLE_WORLD_DB.ANALYTICS.STATION_ACTIVITY ORDER BY STATION_NAME, ZONA_HORARIA, ACTIVITY_TYPE, BIKE_COLOR LIMIT {limit2};"
    # *** Cambio: Usar run_query ***
    df_reporte2 = run_query(query2)
    if not df_reporte2.empty: st.dataframe(df_reporte2, use_container_width=True)
    elif limit2 > 0 : st.warning("No se pudieron cargar los datos para el Reporte 2 o la vista está vacía.")
    st.markdown("---")

    # --- Reporte 3: Actividad por Estación (Marylebone) ---
    st.markdown("**3. Reporte de actividades en Marylebone**")
    limit3_input = st.number_input(
        label="Número de filas a mostrar:", min_value=1, max_value=10000, value=100, step=50,
        key='limit_reporte_3_num', help="Introduce el número de filas que deseas visualizar (máx 10000)."
    )
    limit3 = int(limit3_input)
    query3 = f"SELECT * FROM CYCLE_WORLD_DB.ANALYTICS.STATION_ACTIVITY_MARYLEBONE ORDER BY STATION_NAME, ZONA_HORARIA, ACTIVITY_TYPE, BIKE_COLOR LIMIT {limit3};"
    # *** Cambio: Usar run_query ***
    df_reporte3 = run_query(query3)
    if not df_reporte3.empty: st.dataframe(df_reporte3, use_container_width=True)
    elif limit3 > 0 : st.warning("No se pudieron cargar los datos para el Reporte 3 o la vista está vacía.")


# --- Página 2: Dashboard Analítico (Usa run_query, gráficos st.bar_chart) ---
elif page == page_options[1]: 
    
    st.title("📊 Dashboard Cycle World") 
    st.write("Análisis visual de la operativa y factores externos.")
    st.markdown("---") 

    col1, col2 = st.columns(2) 

    with col1: 
        st.subheader("🏆 Top 10 Estaciones Más Concurridas")
        query_top10 = "SELECT STATION_NAME, TOTAL_ACTIVITY FROM CYCLE_WORLD_DB.ANALYTICS.TOP_TEN_STATIONS;"
        # *** Cambio: Usar run_query ***
        df_top10 = run_query(query_top10)
        if not df_top10.empty:
            # ... (código st.metric) ...
            top_station_name = df_top10.iloc[0]['STATION_NAME']
            top_station_activity = df_top10.iloc[0]['TOTAL_ACTIVITY']
            st.metric(label="Estación #1 Más Concurrida", value=top_station_name, delta=f"{top_station_activity:,} actividades", delta_color="off")
            st.write(" ") 
            df_top10_chart = df_top10.set_index('STATION_NAME')
            st.bar_chart(df_top10_chart['TOTAL_ACTIVITY']) # Gráfico por defecto
        else: st.warning("No se pudieron cargar datos para el Top 10 de estaciones.")

    with col2: 
        st.subheader("🎨 Uso de Bicicletas por Color")
        query_colors = "SELECT BIKE_COLOR, JOURNEY_COUNT FROM CYCLE_WORLD_DB.ANALYTICS.BIKE_COLOR;" 
        # *** Cambio: Usar run_query ***
        df_colors = run_query(query_colors)
        if not df_colors.empty:
            # ... (código st.metric) ...
            most_used_color = df_colors.iloc[0]['BIKE_COLOR']
            most_used_count = df_colors.iloc[0]['JOURNEY_COUNT']
            st.metric(label="Color Más Usado", value=most_used_color, delta=f"{most_used_count:,} viajes", delta_color="off")
            st.write(" ") 
            df_colors_chart = df_colors.set_index('BIKE_COLOR')
            st.bar_chart(df_colors_chart['JOURNEY_COUNT']) # Gráfico por defecto
        else: st.warning("No se pudieron cargar datos del uso de bicicletas por color.")

    st.markdown("---") 

    st.subheader("Impacto del Clima en los Viajes")
    col3, col4 = st.columns(2)
    with col3: 
        query_rainy = "SELECT PERCENTAGE_RAINY FROM CYCLE_WORLD_DB.ANALYTICS.RAINY_PERCENTAGE;"
        # *** Cambio: Usar run_query ***
        df_rainy = run_query(query_rainy)
        if not df_rainy.empty and 'PERCENTAGE_RAINY' in df_rainy.columns : 
            percentage = df_rainy['PERCENTAGE_RAINY'].iloc[0]
            st.metric(label="🌧️ % Viajes en Días Lluviosos", value=f"{percentage:.1f}%" if percentage is not None else "N/A", delta=None) 
        else: st.warning("No se pudo calcular el % de viajes lluviosos.")
            
    with col4: 
        query_sunny = "SELECT AVG_DURATION_MINUTES_CLEAR_DAYS FROM CYCLE_WORLD_DB.ANALYTICS.AVG_JOURNEY_SUNNY;"
        # *** Cambio: Usar run_query ***
        df_sunny = run_query(query_sunny)
        column_name_sunny = 'AVG_DURATION_MINUTES_CLEAR_DAYS' 
        if not df_sunny.empty and column_name_sunny in df_sunny.columns:
            avg_duration = df_sunny[column_name_sunny].iloc[0]
            st.metric(label="☀️ Duración Promedio (Días Despejados)", value=f"{avg_duration:.1f} min" if avg_duration is not None else "N/A", delta=None)
        else: st.warning("No se pudo calcular la duración promedio (días despejados).")

    st.markdown("---")
    
    st.subheader("Disponibilidad de Bicicletas (Análisis Limitado)")
    st.warning("""
    **Pregunta:** ¿Hubo algún día en donde alguna estación haya quedado sin bicicletas disponibles?
    **Respuesta:** No se puede determinar con fiabilidad... (etc.)
    """)

# --- FIN DEL CÓDIGO ---

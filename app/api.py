from fastapi import FastAPI, Query
import requests
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from datetime import timedelta


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite solicitudes desde cualquier origen
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos
    allow_headers=["*"],  # Permite todos los headers
)


# Definir el endpoint
@app.get("/obtener_datos_firms/")
async def obtener_datos_firms(
    country: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    """
    Este servicio consume la API de FIRMS para un país y rango de fechas específicos.
    - country: Código del país (ejemplo: 'ARG')
    - start_date: Fecha de inicio (ejemplo: '2023-01-01')
    - end_date: Fecha de fin (ejemplo: '2023-01-10')
    """
    
    # Convertir las fechas en objetos datetime
    fecha_inicio = datetime.strptime(start_date, '%Y-%m-%d')
    fecha_fin = datetime.strptime(end_date, '%Y-%m-%d')

    # Calcular la diferencia en días
    delta_dias = (fecha_fin - fecha_inicio).days + 1  # +1 para incluir el día final

    # Verificar que el rango no exceda los 10 días
    if delta_dias > 10:
        return {"error": "El rango de fechas no puede exceder los 10 días."}

    # URL para descargar los datos de FIRMS en formato CSV para el rango de fechas
    api_key = "7d41d47b3c83b87d4753af72e55eae00"
    url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{api_key}/VIIRS_NOAA20_NRT/{country}/{delta_dias}/{start_date}"

    # Hacer la solicitud para obtener los datos
    response = requests.get(url)
    
    if response.status_code == 200:
        # Procesar el contenido CSV
        csv_data = response.text
        
        # Convertir el contenido CSV en un DataFrame de pandas
        df = pd.read_csv(StringIO(csv_data))

        # Convertir la latitud y longitud a valores numéricos, forzando a NaN los que no lo sean
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        # Eliminar filas con valores NaN en latitud o longitud
        df = df.dropna(subset=['latitude', 'longitude'])

        # Definir una función para aplicar el truncamiento por cada grupo de fecha y tiempo
        def eliminar_duplicados_por_fecha_y_tiempo(grupo):
            # Truncar latitud y longitud al primer decimal
            grupo['lat_truncated'] = np.floor(grupo['latitude'] * 10) / 10
            grupo['lon_truncated'] = np.floor(grupo['longitude'] * 10) / 10
            
            # Eliminar duplicados basados en la latitud y longitud truncadas, manteniendo la primera ocurrencia
            grupo = grupo.drop_duplicates(subset=['lat_truncated', 'lon_truncated'], keep='first')

            # Eliminar las columnas temporales usadas para el truncamiento
            grupo = grupo.drop(columns=['lat_truncated', 'lon_truncated'])

            return grupo

        # Aplicar el proceso de eliminación de duplicados para cada combinación de fecha y tiempo
        df_final = df.groupby(['acq_date', 'acq_time']).apply(eliminar_duplicados_por_fecha_y_tiempo).reset_index(drop=True)

        # Retornar los datos finales
        return df_final.to_dict(orient="records")

    else:
        return {"error": "No se pudo obtener los datos desde la API."}

# Definir la API Key de OpenWeatherMap
API_KEY = "e07702c7e527cafdc7e1e338dfa9a664"

def obtener_datos_calidad_aire_historico(lat, lon, start, end):
    """
    Obtener datos históricos de calidad del aire usando la API de OpenWeatherMap.
    """
    url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={API_KEY}"
    
    respuesta = requests.get(url)
    
    if respuesta.status_code == 200:
        datos = respuesta.json()
        return datos
    else:
        print("Error al obtener los datos:", respuesta.status_code)
        return None

def calcular_promedio_componentes(datos):
    """
    Calcular el promedio de los componentes de calidad del aire.
    """
    total = {}
    count = len(datos)

    for item in datos:
        for component, value in item["components"].items():
            if component not in total:
                total[component] = 0
            total[component] += value

    promedio = {component: total[component] / count for component in total}
    
    return promedio

@app.get("/calidad_aire/historico/")
async def calidad_aire_historico(lat: float = Query(...), lon: float = Query(...), 
                            fecha_inicio: str = Query(...), fecha_fin: str = Query(...)):
    """
    Este servicio obtiene los datos históricos de calidad del aire para una ubicación específica en un rango de fechas.
    - lat: Latitud de la ubicación
    - lon: Longitud de la ubicación
    - fecha_inicio: Fecha de inicio en formato 'YYYY-MM-DD'
    - fecha_fin: Fecha de fin en formato 'YYYY-MM-DD'
    """
    # Convertir las fechas a timestamp
    fecha_inicio_datetime = datetime.strptime(fecha_inicio, "%Y-%m-%d")
    fecha_fin_datetime = datetime.strptime(fecha_fin, "%Y-%m-%d")
    
    # Obtener los timestamps en segundos
    start_timestamp = int(fecha_inicio_datetime.timestamp())
    end_timestamp = int(fecha_fin_datetime.timestamp()) + 86399  # Añadir 86399 para incluir todo el último día

    # Obtener datos históricos de calidad del aire
    datos_calidad_aire = obtener_datos_calidad_aire_historico(lat, lon, start_timestamp, end_timestamp)
    
    if datos_calidad_aire and "list" in datos_calidad_aire:
        # Calcular el promedio de los componentes de calidad del aire
        promedio_componentes = calcular_promedio_componentes(datos_calidad_aire["list"])
        
        # Obtener el promedio del AQI
        promedio_aqi = np.mean([item["main"]["aqi"] for item in datos_calidad_aire["list"]])

        return {
            "promedio_aqi": promedio_aqi,
            "promedio_componentes": promedio_componentes
        }
    else:
        return {"error": "No se pudo obtener la información de calidad del aire."}
    

@app.get("/obtener_imagenes/")
async def obtener_imagenes(
    lat: float = Query(..., description="Latitud (-90 a 90)"),
    lon: float = Query(..., description="Longitud (-180 a 180)"),
    fecha: str = Query(..., description="Fecha en formato YYYY-MM-DD")
):
    """
    Este endpoint genera enlaces a imágenes satelitales basados en la latitud, longitud y fecha proporcionadas.
    - lat: Latitud de la ubicación.
    - lon: Longitud de la ubicación.
    - fecha: Fecha en formato YYYY-MM-DD.
    """
    # Validar y convertir la fecha
    try:
        date = datetime.strptime(fecha, "%Y-%m-%d").date()  # Convertir fecha correctamente
    except ValueError:
        return {"error": "Formato de fecha inválido. Usa 'YYYY-MM-DD'."}

    # Definir el nombre de la capa
    layer_name = 'VIIRS_SNPP_CorrectedReflectance_TrueColor'

    # Crear un cuadro de 1.5 grados alrededor del punto central
    lat_min = lat - 0.7
    lat_max = lat + 0.7
    lon_min = lon - 0.7
    lon_max = lon + 0.7
    extents = f"{lon_min},{lat_min},{lon_max},{lat_max}"  # Ajustar el orden de los límites

    # Fechas de interés: 10 días antes, la fecha exacta, y 10 días después
    dates = [
        date - timedelta(days=7),  # 10 días antes
        date,                        # fecha exacta
        date + timedelta(days=7)   # 10 días después
    ]

    # Lista para almacenar los enlaces de las imágenes
    image_links = []

    # Generar los enlaces para cada fecha
    for currentdate in dates:
        # Construir URL de la imagen
        url = (
            f"https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi?"
            f"version=1.3.0&service=WMS&request=GetMap&format=image/png&"
            f"STYLE=default&bbox={extents}&CRS=EPSG:4326&HEIGHT=512&WIDTH=512&"
            f"TIME={currentdate}&layers={layer_name}"
        )
        image_links.append(url)  # Almacenar el enlace de la imagen

    # Retornar solo los enlaces generados
    return image_links
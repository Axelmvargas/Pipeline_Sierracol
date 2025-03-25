import pandas as pd
import requests
import time
import logging

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_KEY = "7e728c5e2d0619a44e1f8c31a1255eeb"  
ARCHIVO_CSV = "climate-risk-index-1.csv"
ARCHIVO_SALIDA = "datos_unidos.csv"

ciudades_alternativas = {
    "South Africa": ["Pretoria", "Cape Town", "Johannesburg"],
    "Republic of Serbia": "Belgrade",
    "The Bahamas": "Nassau",
    "South Korea": ["Seoul", "Busan"],
    "Republic of Congo": "Brazzaville",
    "Cape Verde": "Praia",
    "Saint Kitts and Nevis": "Basseterre",
}

def obtener_clima(ciudad, api_key):
    """Obtiene datos climáticos de la API de OpenWeatherMap."""
    if isinstance(ciudad, list):
        for city in ciudad:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
            try:
                respuesta = requests.get(url)
                respuesta.raise_for_status()
                return respuesta.json()
            except requests.exceptions.RequestException as e:
                logging.error(f"Error al obtener datos para {city}: {e}")
        return None
    else:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={api_key}&units=metric"
        try:
            respuesta = requests.get(url)
            respuesta.raise_for_status()
            return respuesta.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error al obtener datos para {ciudad}: {e}")
            return None

def procesar_datos():
    """Procesa los datos climáticos y los combina con el DataFrame original."""
    try:
        df_csv = pd.read_csv(ARCHIVO_CSV)
    except FileNotFoundError:
        logging.error(f"No se encontró el archivo: {ARCHIVO_CSV}")
        return

    datos_clima = []
    for pais in df_csv["rw_country_name"]:
        ciudad = ciudades_alternativas.get(pais, pais)
        datos = obtener_clima(ciudad, API_KEY)
        if datos:
            datos_clima.append({
                "pais": pais,
                "temperatura": datos["main"]["temp"],
                "humedad": datos["main"]["humidity"],
                "descripcion": datos["weather"][0]["description"]
            })
        else:
            logging.warning(f"No se encontraron datos para {pais}")
        time.sleep(1)

    df_clima = pd.DataFrame(datos_clima)
    df_unido = pd.merge(df_csv, df_clima, left_on="rw_country_name", right_on="pais")

    try:
        df_unido.to_csv(ARCHIVO_SALIDA, index=False)
        logging.info(f"Datos guardados en {ARCHIVO_SALIDA}")
    except Exception as e:
        logging.error(f"Error al guardar el archivo: {e}")

    print(df_unido.head())

if __name__ == "__main__":
    procesar_datos()
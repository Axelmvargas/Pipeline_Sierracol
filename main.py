import pandas as pd
import requests
import time
import logging
from google.cloud import bigquery
import os
from google.api_core import exceptions
from google.api_core import retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CREDENTIALS_PATH = r"C:\Users\axelm\OneDrive\Documentos\Desarrollos\Prueba_Sierracol\natural-cistern-454823-m8-65cfa8c8881c.json"
API_KEY = "7e728c5e2d0619a44e1f8c31a1255eeb"
ARCHIVO_CSV = "climate-risk-index-1.csv"
PROJECT_ID = "natural-cistern-454823-m8"
DATASET_ID = "Sierracol001"
TABLE_ID = "cruce_inicial002"

ciudades_alternativas = {
    "South Africa": ["Pretoria", "Cape Town", "Johannesburg"],
    "Republic of Serbia": "Belgrade",
    "The Bahamas": "Nassau",
    "South Korea": ["Seoul", "Busan"],
    "Republic of Congo": "Brazzaville",
    "Cape Verde": "Praia",
    "Saint Kitts and Nevis": "Basseterre",
}

@retry.Retry(predicate=retry.if_exception_type(requests.exceptions.RequestException))
def obtener_clima(ciudad, api_key):
    """Obtiene datos climáticos de la API de OpenWeatherMap con reintentos."""
    if isinstance(ciudad, list):
        for city in ciudad:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
            respuesta = requests.get(url)
            respuesta.raise_for_status()
            return respuesta.json()
        return None
    else:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={api_key}&units=metric"
        respuesta = requests.get(url)
        respuesta.raise_for_status()
        return respuesta.json()

@retry.Retry(predicate=retry.if_exception_type(exceptions.ServiceUnavailable))
def cargar_datos_bigquery(client, df_unido, table_ref, job_config):
    """Carga datos en BigQuery con reintentos."""
    job = client.load_table_from_dataframe(df_unido, table_ref, job_config=job_config)
    job.result()

def procesar_datos():
    """Procesa los datos climáticos y los carga en BigQuery."""
    try:
        df_csv = pd.read_csv(ARCHIVO_CSV)
    except FileNotFoundError:
        logging.error(f"No se encontró el archivo: {ARCHIVO_CSV}")
        return

    datos_clima = []
    for pais in df_csv["rw_country_name"]:
        ciudad = ciudades_alternativas.get(pais, pais)
        try:
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
        except Exception as e:
            logging.error(f"Error al obtener datos para {pais}: {e}")
        time.sleep(1)

    df_clima = pd.DataFrame(datos_clima)
    df_unido = pd.merge(df_csv, df_clima, left_on="rw_country_name", right_on="pais")

    # Eliminar las columnas no deseadas y valores nulos 
    columnas_a_eliminar = ['index', 'cartodb_id', 'the_geom', 'the_geom_webmercator', 'rw_country_name', 'pais']
    df_unido.drop(columnas_a_eliminar, axis=1, errors='ignore', inplace=True)
    
    df_unido = df_unido[df_unido['rw_country_code'].notna()]
    
    mediana_losses_gdp = df_unido['losses_per_gdp__total'].median()
    df_unido['losses_per_gdp__total'].fillna(mediana_losses_gdp, inplace=True)    

    client = bigquery.Client.from_service_account_json(CREDENTIALS_PATH, project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        cargar_datos_bigquery(client, df_unido, table_ref, job_config)
        logging.info(f"Datos cargados en BigQuery: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        print(df_unido.head())
    except Exception as e:
        logging.error(f"Error al cargar datos en BigQuery: {e}")

if __name__ == "__main__":
    procesar_datos()
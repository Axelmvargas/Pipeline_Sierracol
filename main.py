import logging
import requests
import csv
from google.cloud import bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import time

# Constantes
API_KEY = "7e728c5e2d0619a44e1f8c31a1255eeb"
PROJECT_ID = "natural-cistern-454823-m8"
DATASET_ID = "Sierracol001"
TABLE_ID = "cruce_inicial005"
CREDENTIALS_PATH = "natural-cistern-454823-m8-65cfa8c8881c.json"
BUCKET_NAME = "sierracol01"
ARCHIVO_CSV = "climate-risk-index-1.csv"
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
            except requests.exceptions.RequestException:
                continue
        return None
    else:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={api_key}&units=metric"
        try:
            respuesta = requests.get(url)
            respuesta.raise_for_status()
            return respuesta.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error obteniendo clima para {ciudad}: {e}")
            return None

def transformar_datos(element):
    """Transforma los datos del CSV y añade la información climática"""
    try:
        with open(element, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            datos_transformados = []
            for row in reader:
                pais = row["country"]
                ciudad = ciudades_alternativas.get(pais, pais)
                datos = obtener_clima(ciudad, API_KEY)
                if datos:
                    row["temperatura"] = datos["main"]["temp"]
                    row["humedad"] = datos["main"]["humidity"]
                    row["descripcion"] = datos["weather"][0]["description"]
                    datos_transformados.append(row)
                else:
                    logging.warning(f"No se encontraron datos para {pais}")
                time.sleep(1)
            return datos_transformados
    except FileNotFoundError:
        logging.error(f"No se encontró el archivo: {element}")
        return []

def cargar_datos_bigquery(element, project_id, dataset_id, table_id):
    """Carga los datos en BigQuery"""
    client = bigquery.Client.from_service_account_json(CREDENTIALS_PATH, project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        client.load_table_from_json(element, table_ref, job_config=job_config)
        logging.info(f"Datos cargados en BigQuery: {project_id}.{dataset_id}.{table_id}")
    except Exception as e:
        logging.error(f"Error al cargar datos en BigQuery: {e}")

def run(argv=None):
    """Define el pipeline y lo ejecuta en Dataflow"""
    pipeline_options = PipelineOptions(
        flags=argv,
        project=PROJECT_ID,
        runner='DataflowRunner',
        temp_location=f'gs://{BUCKET_NAME}/temp/',
        region='us-central1', 
        staging_location=f'gs://{BUCKET_NAME}/staging/'
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logging.info(f"Ruta al archivo CSV: gs://{BUCKET_NAME}/{ARCHIVO_CSV}")

        # Lee el archivo CSV desde GCS
        datos_csv = pipeline | 'Leer archivo CSV' >> beam.Create([f"gs://{BUCKET_NAME}/{ARCHIVO_CSV}"])

        # Transforma los datos
        datos_transformados = datos_csv | 'Transformar datos' >> beam.FlatMap(transformar_datos)

        # Carga los datos a BigQuery
        datos_transformados | 'Cargar en BigQuery' >> beam.Map(cargar_datos_bigquery, project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)

if __name__ == '__main__':
    run()

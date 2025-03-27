# Data Pipeline para Procesamiento de Datos Climáticos y Carga a BigQuery

Este script de Python, desarrollado con Apache Beam, extrae datos de un archivo CSV almacenado en Google Cloud Storage (GCS), enriquece estos datos con información climática obtenida de la API de OpenWeatherMap, y luego carga los datos transformados en una tabla de BigQuery.

## Propósito

El propósito principal de este script es:

1.  Leer datos de un archivo CSV que contiene información de países.
2.  Obtener datos climáticos (temperatura, humedad, descripción del clima) para las ciudades correspondientes a cada país utilizando la API de OpenWeatherMap.
3.  Transformar los datos combinando la información del CSV con los datos climáticos.
4.  Cargar los datos transformados en una tabla de BigQuery.

## Prerrequisitos

Antes de ejecutar este script, asegúrate de tener lo siguiente:

* **Google Cloud Platform (GCP) Account:** Necesitarás una cuenta de GCP con los siguientes servicios habilitados:
    * BigQuery API
    * Cloud Storage API
    * Dataflow API
* **Google Cloud SDK (gcloud):** Instalado y configurado en tu máquina.
* **Python 3.x:** Instalado en tu sistema.
* **Bibliotecas Python:** Instala las siguientes bibliotecas usando `pip`:

    ```bash
    pip install google-cloud-bigquery apache-beam requests
    ```

* **Archivo de Credenciales JSON:** Un archivo de credenciales JSON de GCP (por ejemplo, `natural-cistern-454823-m8-65cfa8c8881c.json`) con los permisos necesarios para acceder a BigQuery y Cloud Storage.
* **Archivo CSV en GCS:** Un archivo CSV llamado `climate-risk-index-1.csv` almacenado en un bucket de GCS (`sierracol01`).
* **API Key de OpenWeatherMap:** Una clave API válida de OpenWeatherMap.

## Configuración

1.  **Variables de Entorno:**
    * Es altamente recomendable almacenar las claves y rutas sensibles como variables de entorno.
    * Define las siguientes variables de entorno:
        * `API_KEY`: Tu clave API de OpenWeatherMap.
        * `PROJECT_ID`: El ID de tu proyecto de GCP.
        * `CREDENTIALS_PATH`: La ruta a tu archivo de credenciales JSON.
        * `BUCKET_NAME`: El nombre de tu bucket de GCS.
        * `DATASET_ID`: El ID del dataset de BigQuery.
        * `TABLE_ID`: El ID de la tabla de BigQuery.

    * Ejemplo (Linux/macOS):

        ```bash
        export API_KEY="tu_api_key"
        export PROJECT_ID="natural-cistern-454823-m8"
        export CREDENTIALS_PATH="/ruta/a/natural-cistern-454823-m8-65cfa8c8881c.json"
        export BUCKET_NAME="sierracol01"
        export DATASET_ID="Sierracol001"
        export TABLE_ID="cruce_inicial005"
        ```

    * Ejemplo (Windows):
        * Configura las variables de entorno a través del panel de control de Windows.

2.  **Archivo `.gitignore`:**
    * Crea un archivo `.gitignore` en el directorio raíz de tu proyecto y asegúrate de añadir el archivo de credenciales JSON para evitar que se suba al repositorio.

    ```
    *.json
    ```

## Ejecución

1.  **Ejecutar el script:**

    ```bash
    python tu_script.py --runner DataflowRunner --temp_location gs://sierracol01/temp/ --staging_location gs://sierracol01/staging/ --region us-central1
    ```

    * Asegúrate de reemplazar `tu_script.py` con el nombre de tu script.

2.  **Monitorear el trabajo en Dataflow:**

    * Puedes monitorear el progreso del trabajo en la consola de Google Cloud, en la sección de Dataflow.

## Estructura del Script

* **Constantes:** Definición de claves, rutas y nombres de tablas.
* **`obtener_clima(ciudad, api_key)`:** Función para obtener datos climáticos de la API de OpenWeatherMap.
* **`transformar_datos(element)`:** Función para transformar los datos del CSV y añadir la información climática.
* **`cargar_datos_bigquery(element, project_id, dataset_id, table_id)`:** Función para cargar los datos en BigQuery.
* **`run(argv=None)`:** Función principal que define y ejecuta el pipeline de Apache Beam.

## Notas

* El script incluye manejo de errores y logging para ayudar a la depuración.
* Se utiliza `time.sleep(1)` para evitar sobrecargar la API de OpenWeatherMap.
* El script está configurado para truncar la tabla de BigQuery cada vez que se ejecuta (`WRITE_TRUNCATE`).
* Asegúrese de que el archivo csv se encuentre en el bucket de google cloud storage especificado.

Este `README.md` proporciona una guía clara sobre cómo configurar y ejecutar tu script, asegurando que otros puedan entender y utilizar tu código fácilmente.

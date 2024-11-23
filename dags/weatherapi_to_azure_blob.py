from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime
import json


openweather_api_key = Variable.get("OPENWEATHER_API_KEY")

# Define latitude and longitude as variables
latitude = 41.14961
longitude = -8.61099

# Process data
def process_weather_data(ti):
    data = ti.xcom_pull(task_ids='get_weather_data')
    data = json.loads(data)

    country = data["sys"]["country"]
    city = data["name"]
    
    # Transformations
    processed_data = {
        "country": data["sys"]["country"],
        "city": data["name"],
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "timestamp": data["dt"]
    }

    # Save processed data as a temp file
    output_path = '/tmp/processed_weather_data.json'
    with open(output_path, 'w') as f:
        json.dump(processed_data, f)
    
    ti.xcom_push(key='country', value=country)
    ti.xcom_push(key='city', value=city)
    ti.xcom_push(key='output_path', value=output_path)

# Upload to Azure Blob Storage
def upload_to_azure_blob(ti, container_name, execution_date):
    # Get file path, country and city
    file_path = ti.xcom_pull(key='output_path', task_ids='process_weather_data')
    country = ti.xcom_pull(key='country', task_ids='process_weather_data')
    city = ti.xcom_pull(key='city', task_ids='process_weather_data')
 
    # Format the blob name acording to country and city
    blob_name = f'{country}_{city}_weather_data/{execution_date}_{country}_{city}_processed_weather_data.json'
    
    hook = WasbHook(wasb_conn_id='azure_blob_storage')
    hook.load_file(file_path, container_name, blob_name, overwrite=False)

default_args = {
    'start_date': datetime(2024, 1, 1)
}


with DAG(dag_id='weather_data_pipeline',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False
         ) as dag:
    
    # Task 1: Get data from5 API OpenWeather by latitude and longitude
    get_weather_data = SimpleHttpOperator(
        task_id='get_weather_data',
        http_conn_id='openweather_api',
        endpoint=f'/data/2.5/weather?lat={latitude}&lon={longitude}&appid={openweather_api_key}',
        method='GET',
        log_response=True
    )

    # Task 2: Process Weather Data
    process_data = PythonOperator(
        task_id='process_weather_data',
        python_callable=process_weather_data,
    )

    # Task 3: Load processed data to Azure Blob Storage
    upload_to_blob = PythonOperator(
        task_id='upload_to_blob',
        python_callable=upload_to_azure_blob,
        op_kwargs={
            'container_name': 'tst',
            'execution_date': '{{ ts }}'  
        }
    )
    
    
    # Task dependency
    get_weather_data >> process_data >> upload_to_blob
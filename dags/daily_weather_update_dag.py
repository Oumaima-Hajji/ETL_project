import json

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from pendulum import datetime


def call_api():
    """
    This function calls the Weather API and returns the data in a json format
    Input : None
    Output :  data (weather data in the form of a json)
    """
    #url = "https://api.open-meteo.com/v1/forecast?latitude=48.85&longitude=2.35&hourly=rain,windspeed_80m,temperature_80m&daily=uv_index_max,precipitation_sum,rain_sum,snowfall_sum,precipitation_hours&forecast_days=16&start_date=2023-02-01&end_date=2023-06-14&timezone=GMT"
    url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m,precipitation_probability,rain,snowfall,snow_depth,cloudcover,visibility,windspeed_10m,winddirection_10m,temperature_80m,soil_temperature_6cm,soil_temperature_18cm"

    response = requests.get(url=url)
    data = response.json()
    return data


def upload_json_to_gcs(bucket_name, file_name, json_data, key_file_path):
    """
    This function uploads the json file in a Google cloud Storage Bucket
    Input : bucket_name , file_name , json_data , key_file_path
    Output : json_data uploaded as file_name in bucket_name using the service account key_file_path
    """
    credentials = service_account.Credentials.from_service_account_file(key_file_path)
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    blob.upload_from_string(json.dumps(json_data), content_type="application/json")
    print(f"Successfully uploaded {file_name} to {bucket_name}")



def upload_json_to_gcs_bucket(dag_execution_date):
    """
    This function gives the upload_json_to_gcs function its arguments so it could be used in a python_callable parameter
    Input : dag_execution_date is added to the file_name in order to get the date of the weather data file
    Output : upload_json_to_gcs Execution
    """
    bucket_name = "weather_data_oumaima"
    file_name = f"weather_data_from_api_{dag_execution_date}.json"
    json_data = call_api()

    key_file_path = "plugins/credentials.json"

    upload_json_to_gcs(bucket_name, file_name, json_data, key_file_path)



def check_data_already_exist(dag_execution_date):

    '''
    This function should check the table and sees if there is data or not (false , true) 
    -> hourly.time

    '''
    client = bigquery.Client.from_service_account_json('plugins/credentials.json')


    # Define your SQL query
    sql_query = f'''
        SELECT *
        FROM (
        SELECT TIMESTAMP_TRUNC(timestamp, HOUR) AS date_hour
        FROM `liquid-virtue-382517.daily_weather_data.daily_weather_data_table`,
            UNNEST(hourly.time) AS timestamp
        )
        WHERE DATE(date_hour) = DATE('{dag_execution_date}')
        '''
    # Execute the query
    query_job = client.query(sql_query)

    # Get the result
    result = query_job.result()
  
    # Check if there is data or not
    if result.total_rows > 0:
        return 'skipping'  # Data exists
    else:
        return 'gcs_to_bigquery_operator'  # No data

    




with DAG(
    dag_id="daily_weather_update",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",  # cron
    max_active_runs=2,
    catchup=False,
) as dag:
    ds = "{{ ds }}"
    begin = EmptyOperator(task_id="begin")

    call_api_json = PythonOperator(
        task_id="call_api",
        python_callable=call_api,
    )

    def give_execution_date(ds):
        return upload_json_to_gcs_bucket(dag_execution_date=ds)

    data_to_gcs = PythonOperator(
        task_id="upload_data_to_gcs",
        python_callable=give_execution_date,
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id="daily_weather_data",
        project_id="liquid-virtue-382517",
        gcp_conn_id="gcs_connection",
    )

    
    def check_result(ds):
        return check_data_already_exist(dag_execution_date=ds)
    check_if_data_already_exist = BranchPythonOperator(



        task_id="check_if_data_already_exist",
        python_callable=check_result,
        do_xcom_push=False

    )


    dag_execution_date = "{{ ds }}"
    bucket_name = "weather_data_oumaima"
    file_name = f"weather_data_from_api_{dag_execution_date}.json"
    dataset_id = "daily_weather_data"
    table_id = "daily_weather_data_table"

    gcs_to_bigquery_operator = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=bucket_name,
        source_objects=[file_name],
        destination_project_dataset_table=f"{dataset_id}.{table_id}",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        source_format="NEWLINE_DELIMITED_JSON",
        gcp_conn_id="gcs_connection",
    )

    skipping = EmptyOperator(task_id="skipping")
    end = EmptyOperator(task_id="end")

(
    begin
    >> call_api_json
    >> data_to_gcs
    >> create_dataset
    >> check_if_data_already_exist
    >> [gcs_to_bigquery_operator  , skipping ] >> end
    
    
)

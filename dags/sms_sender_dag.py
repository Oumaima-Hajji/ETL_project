import json
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from pendulum import datetime
from send_sms import sms_sender
from schema import schema


def call_people():
    # specific keys : first_name, last_name , city, country , phone number, street, email, uuid (-> unNested fields)

    """
    This function calls the People API and returns the data in a json format
    Input : None
    Output :  data (people data in the form of a json)
    """
    url = "https://randomuser.me/api/"

    response = requests.get(url=url)
    data = response.json()

    people_database = {
        "first_name": str(data["results"][0]["name"]["first"]),
        "last_name": str(data["results"][0]["name"]["last"]),
        "city": str(data["results"][0]["location"]["city"]),
        "street": str(data["results"][0]["location"]["street"]["number"])
        + str(data["results"][0]["location"]["street"]["name"]),
        "country": str(data["results"][0]["location"]["country"]),
        "postcode": str(data["results"][0]["location"]["postcode"]),
        "latitude": str(data["results"][0]["location"]["coordinates"]["latitude"]),
        "longitude": str(data["results"][0]["location"]["coordinates"]["longitude"]),
        "email": str(data["results"][0]["email"]),
        "phone": str(data["results"][0]["phone"]),
        "uuid": str(data["results"][0]["login"]["uuid"]),
    }
    return people_database


def add_execution_date_to_data(data, dag_execution_date_time):
    """
    This function adds the dag_execution_date_time to the data so it could be used in the BigQuery table
    Input : data (people data in the form of a json) , dag_execution_date_time
    Output : data with dag_execution_date_time added
    """
    new_data = {"dag_execution_date_time": f"{dag_execution_date_time}", **data}
    return new_data


def upload_data_to_gcs(bucket_name, file_name, json_data, key_file_path):
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


def upload_data_to_gcs_bucket(dag_execution_date_time):
    """
    This function gives the upload_json_to_gcs function its arguments so it could be used in a python_callable parameter
    Input : dag_execution_date is added to the file_name in order to get the date of the weather data file
    Output : upload_json_to_gcs Execution
    """
    bucket_name = "people_record"
    file_name = f"people_record_{dag_execution_date_time}.json"
    api_data = call_people()
    # print(api_data)
    # print(type(api_data))
    # print(type(dag_execution_date_time))

    json_data = add_execution_date_to_data(
        data=api_data, dag_execution_date_time=dag_execution_date_time
    )

    key_file_path = "plugins/credentials.json"

    upload_data_to_gcs(bucket_name, file_name, json_data, key_file_path)


with DAG(
    dag_id="sms_weather_notification",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",  # "*/5 * * * *",  # or cron
    max_active_runs=2,
    catchup=False,
) as dag:
    ts = "{{ ts }}"
    begin = EmptyOperator(task_id="begin")

    call_people_api = PythonOperator(
        task_id="call_people_record_api",
        python_callable=call_people,
    )

    def upload_people_record_with_execution_date(ts):
        return upload_data_to_gcs_bucket(dag_execution_date_time=ts)

    data_to_gcs = PythonOperator(
        task_id="upload_data_to_gcs",
        python_callable=upload_people_record_with_execution_date,
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id="people_record_data",
        project_id="liquid-virtue-382517",
        gcp_conn_id="gcs_connection",
    )

    bucket_name = "people_record"
    file_name = f"people_record_{ts}.json"
    dataset_id = "people_record_data"
    table_id = "people_record_database_table"

    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=bucket_name,
        source_objects=[file_name],
        destination_project_dataset_table=f"{dataset_id}.{table_id}",
        # autodetect = True ##For first time use
        schema_fields=schema,
        write_disposition="WRITE_APPEND",  # WRITE_APPEND or "WRITE_TRUNCATE"
        create_disposition="CREATE_IF_NEEDED",
        source_format="NEWLINE_DELIMITED_JSON",
        gcp_conn_id="gcs_connection",  # Connection made to Google Cloud through the Airflow UI connection page
    )

    send_sms_notification = PythonOperator(
        task_id="send_sms_notif",
        python_callable=sms_sender,
    )

    end = EmptyOperator(task_id="end")

(
    begin
    >> call_people_api
    >> data_to_gcs
    >> create_dataset
    >> gcs_to_bigquery
    >> send_sms_notification
    >> end
)

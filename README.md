# Daily Weather data ETL pipeline scheduling using Airflow

![Alt text](images/dagidem.jpeg)




Overview
========

This project Schedules a daily call of daily Weather data by calling a Weather API and then loads the json data into a Google Cloud Storage Bucket. After that it puts the data into a BigQuery Table.

And then we visualize the data using Looker.


Overall architecture :

![Alt text2](images/dag_architecture.jpg)

My resulting Dashboard in Looker :

![Alt text3](images/Dashboard.jpeg)

Project Contents
================


My project contains a "dags" folder : 
- This folder contains the Python files for my Airflow DAGs. It includes :
    -  daily_weather_update_dag.py  
        
        The architecture of the DAG is as follows :

        - Begin : `EmptyOperator` 

        - data_to_gcs : `PythonOperator`

        - create_dataset : `BigQueryCreateEmptyDatasetOperator`

        - check_if_data_already_exist : `BranchPythonOperator`

        - gcs_to_bigquery_operator :  `GCSToBigQueryOperator`

        - skipping : `EmptyOperator`

        - End : `EmptyOperator`



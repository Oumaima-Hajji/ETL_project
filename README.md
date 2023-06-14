# Daily Weather data ETL pipeline scheduling using Airflow

![Alt text](images/dags.jpeg)


![Alt text](images/dag_arch.png)


Overview
========

This project Schedules a daily call of daily Weather data by calling a Weather API and then loads the json data into a Google Cloud Storage Bucket. After that it puts the data into a BigQuery Table.

And then we visualize the data using Looker.




Project Contents
================


My project contains a "dags" folder : 
- This folder contains the Python files for my Airflow DAGs. It includes :
    -  daily_weather_update_dag.py  
        
        The architecture of the DAG is as follows :

        - Begin : EmptyOperator 

        - data_to_gcs : PythonOperator

        - create_dataset : BigQueryCreateEmptyDatasetOperator

        - gcs_to_bigquery_operator :  GCSToBigQueryOperator

        - End : EmptyOperator



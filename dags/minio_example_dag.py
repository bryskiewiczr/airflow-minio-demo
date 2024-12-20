import os
import csv
from datetime import datetime, timedelta
import logging
from pathlib import Path
import pendulum
import urllib3

from minio import Minio
from minio.error import S3Error

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import pyarrow.csv as pcsv
import pyarrow.parquet as pq


# Define DAG
@dag(
    dag_id="minio_example_dag",
    description="do some stuff with MinIO and Postgres working on CSV files",
    schedule=None,
    start_date=pendulum.datetime(2024, 12, 18, tz="UTC"),
    catchup=False,
    tags=["minio"],
)
def dag():
    # Initialize MinIO configuration
    minio_config = {
        "csv_bucket": "csv-files",
        "parquet_bucket": "parquet-files",
        "minio_endpoint": str(os.getenv("MINIO_ENDPOINT_URL")),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY_ID"),
        "minio_secret_key": os.getenv("MINIO_SECRET_ACCESS_KEY"),
        "minio_region": os.getenv("MINIO_REGION_NAME")
    }

    # Initialize MinIO client
    minio_client = Minio(
        endpoint=minio_config["minio_endpoint"],
        access_key=minio_config["minio_access_key"],
        secret_key=minio_config["minio_secret_key"],
        region=minio_config["minio_region"],
        secure=False,
    )

    # Filepath variables
    csv_file_name = "food-price-index-september-2023-weighted-average-prices.csv"
    csv_file_path = f"/opt/airflow/data/in/{csv_file_name}"
    parquet_file_name = "food-price-index-september-2023-weighted-average-prices.parquet"
    parquet_file_path = f"/opt/airflow/data/out/{parquet_file_name}"

    # Define tasks
    # Reusable task to put file to MinIO - with bucket_name and file_name and file_path as parameters            
    @task
    def put_file_to_minio(bucket_name="", file_name="", file_path=""):
        if not bucket_name or not file_name or not file_path:
            raise AirflowException("Missing parameters in function `put_file_to_minio` call.")

        # Make sure the bucket exists
        bucket_exists = minio_client.bucket_exists(bucket_name)
        if not bucket_exists:
            minio_client.make_bucket(bucket_name)
            logging.info("Bucket %s not found initially, but has now been created.", bucket_name) 
        else:
            logging.info("Bucket %s already exists", bucket_name)
            
        # Put the file to MinIO
        try:
            minio_client.fput_object(bucket_name, file_name, file_path)
            logging.info("%s uploaded to %s bucket.", file_name, bucket_name)
        except S3Error as err:
            logging.error(err)

    @task
    def convert_csv_to_parquet():
        # Convert csv to parquet using pandas
        table = pcsv.read_csv(csv_file_path)
        pq.write_table(table, parquet_file_path)        

    # Simple SQLExecuteQueryOperator to create target table from .sql script file
    create_table = SQLExecuteQueryOperator(
        task_id="create_food_price_index_table",
        conn_id="postgres-conn",
        sql="sql/create_food_price_index_table.sql"
    )

    # Bash task to copy .csv file to Postgres using `psql` bash command
    @task.bash
    def insert_data_to_postgres():
        return f"""
            PGPASSWORD=airflow psql -h postgres -U airflow -d airflow \
            -c "\copy public.food_price_index FROM {csv_file_path} DELIMITER ',' CSV HEADER;"
            """

    # Define task execution order
    put_file_to_minio.override(task_id="put_csv_to_minio")(minio_config["csv_bucket"], csv_file_name, csv_file_path) \
    >> convert_csv_to_parquet() \
    >> put_file_to_minio.override(task_id="put_parquet_to_minio")(minio_config["parquet_bucket"], parquet_file_name, parquet_file_path) \
    >> create_table \
    >> insert_data_to_postgres()


# Instantiate the DAG
dag()

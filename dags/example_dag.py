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
import pyarrow.csv as pcsv
import pyarrow.parquet as pq


@dag(
    dag_id="minio",
    description="do some stuff with MinIO and Postgres working on CSV files",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 12, 18, tz="UTC"),
    catchup=False,
    tags=["minio"],
)
def dag():
    # http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
    # urllib3.disable_warnings()
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
    
    csv_file_name = "food-price-index-september-2023-weighted-average-prices.csv"
    csv_file_path = f"/opt/airflow/data/in/{csv_file_name}"
    
    parquet_file_name = "food-price-index-september-2023-weighted-average-prices.parquet"
    parquet_file_path = f"/opt/airflow/data/out/{parquet_file_name}"
    
    @task
    def put_csv_to_minio():
        # Define some variables
        bucket_name = minio_config["csv_bucket"]
        
        # Make sure the bucket exists
        bucket_exists = minio_client.bucket_exists(bucket_name)
        if not bucket_exists:
            minio_client.make_bucket(bucket_name)
            logging.info("Bucket %s not found initially, but has now been created.", bucket_name) 
        else:
            logging.info("Bucket %s already exists", bucket_name)
            
        # Put the file to MinIO
        try:
            minio_client.fput_object(bucket_name, csv_file_name, csv_file_path)
            logging.info("%s uploaded to %s bucket.", csv_file_name, bucket_name)
        except S3Error as err:
            logging.error(err)
            
    @task
    def convert_csv_to_parquet():
        # Convert csv to parquet using pandas
        table = pcsv.read_csv(csv_file_path)
        pq.write_table(table, parquet_file_path)
            
    @task
    def put_parquet_to_minio():
        # Define some variables
        bucket_name = minio_config["parquet_bucket"]
        
        # Make sure the bucket exists
        bucket_exists = minio_client.bucket_exists(bucket_name)
        if not bucket_exists:
            minio_client.make_bucket(bucket_name)
            logging.info("Bucket %s not found initially, but has now been created.", bucket_name) 
        else:
            logging.info("Bucket %s already exists", bucket_name)
            
        # Put the file to MinIO
        try:
            minio_client.fput_object(bucket_name, parquet_file_name, parquet_file_path)
            logging.info("%s uploaded to %s bucket.", parquet_file_name, bucket_name)
        except S3Error as err:
            logging.error(err)
    
    put_csv_to_minio() >> convert_csv_to_parquet() >> put_parquet_to_minio()
    
    
dag()

import csv
from datetime import datetime, timedelta
import urllib3

from minio import Minio

from airflow.decorators import dag, task

with dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 12, 18, tz="UTC"),
    catchup=False,
    tags=["minio"],
) as dag:

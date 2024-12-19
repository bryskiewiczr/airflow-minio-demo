from airflow.decorators import dag, task
import csv
import urllib3
from minio import Minio


with dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 12, 18, tz="UTC"),
    catchup=False,
    tags=["minio"],

) as dag:

config = {
  "dest_bucket":    "airflowcsv", # This will be auto created
  "minio_endpoint": "http://localhost:9001",
  "minio_username": "minio99",
  "minio_password": "minio123",
}

# Since we are using self-signed certs we need to disable TLS verification
http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
urllib3.disable_warnings()

# Initialize MinIO client
minio_client = Minio(config["minio_endpoint"],
               secure=True,
               access_key=config["minio_username"],
               secret_key=config["minio_password"],
               http_client = http_client
               )

# Create destination bucket if it does not exist
#if not minio_client.bucket_exists(config["dest_bucket"]):
#  minio_client.make_bucket(config["dest_bucket"])
#  print("Destination Bucket '%s' has been created" % (config["dest_bucket"]))

      minio_client.fput_object(config["dest_bucket"], object_path, object_path)
      print("- Uploaded processed object '%s' to Destination Bucket '%s'" % (object_path, config["dest_bucket"]))
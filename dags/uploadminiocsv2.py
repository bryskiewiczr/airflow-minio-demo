# -*- coding: utf-8 -*-
# MinIO Python Library for Amazon S3 Compatible Cloud Storage,
# (C) 2015 MinIO, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta
from minio import Minio
from minio.commonconfig import GOVERNANCE, Tags
from minio.retention import Retention
from minio.sse import SseCustomerKey, SseKMS, SseS3

with DAG(
    schedule=None,
    start_date=pendulum.datetime(2024, 12, 18, tz="UTC"),
    catchup=False,
    tags=["minio"],

):

    client = Minio(
        dest_bucket="airflowcsv",
        minio_endpoint="http://localhost:9001",
        minio_username="minio99",
        minio_password="minio123"
    )

    # Upload data.
    result = client.fput_object(
        "my-bucket", "my-object", "my-filename",
)
print(
    "created {0} object; etag: {1}, version-id: {2}".format(
        result.object_name, result.etag, result.version_id,
    ),
)
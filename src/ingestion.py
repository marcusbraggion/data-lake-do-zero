# imports
import datetime
import os

import boto3
import pandas as pd
from dotenv import load_dotenv

# .env
load_dotenv()

region_name = os.getenv("AWS_REGION")
aws_access_key_id = os.getenv("AWS_ACCESS_KEY")
aws_secret_access_key = os.getenv("AWS_SECRET_KEY")
bucket_name = os.getenv("AWS_BUCKET_STORES_RAW")


def upload_csv_to_s3():
    # Criar uma sessão no S3
    now = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
    s3 = boto3.resource(
        service_name="s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    s3.meta.client.upload_file(
        Filename="./data/store.csv",
        Bucket=bucket_name,
        Key=f"raw/stores_raw_{now}.csv",
    )

upload_csv_to_s3()
import json
import os
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]


def load_environment():
    load_dotenv(ROOT_DIR / ".env")


def get_bucket_name():
    return os.getenv("MINIO_BUCKET", "plaid-data")


def get_s3_client():
    load_environment()

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_bucket(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return
    except ClientError as error:
        status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        error_code = error.response.get("Error", {}).get("Code")

        if status_code not in {404, 400} and error_code not in {
            "404",
            "NoSuchBucket",
            "NotFound",
        }:
            raise

    s3_client.create_bucket(Bucket=bucket_name)


def put_json_object(s3_client, bucket_name, key, payload):
    body = json.dumps(payload, indent=2, default=str).encode("utf-8")

    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


def get_json_object(s3_client, bucket_name, key):
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def object_exists(s3_client, bucket_name, key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as error:
        status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        error_code = error.response.get("Error", {}).get("Code")

        if status_code == 404 or error_code in {"404", "NoSuchKey", "NotFound"}:
            return False

        raise


def list_object_keys(s3_client, bucket_name, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]

            if not key.endswith("/"):
                yield key

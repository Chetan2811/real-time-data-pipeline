import argparse
import sys
import time
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError


ROOT_DIR = Path(__file__).resolve().parents[1]
APP_DIR = ROOT_DIR / "app"
DEFAULT_SAMPLE_FILE = ROOT_DIR / "data" / "spark" / "raw" / "sample_transactions.jsonl"
DEFAULT_OBJECT_KEY = "spark/raw/sample_transactions.jsonl"

sys.path.insert(0, str(APP_DIR))

from minio_storage import ensure_bucket, get_bucket_name, get_s3_client  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload the local Spark sample JSONL file to MinIO."
    )
    parser.add_argument(
        "--file",
        default=str(DEFAULT_SAMPLE_FILE),
        help="Local sample JSONL file to upload.",
    )
    parser.add_argument(
        "--key",
        default=DEFAULT_OBJECT_KEY,
        help="MinIO object key to write.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=10,
        help="Number of attempts while waiting for MinIO.",
    )
    return parser.parse_args()


def upload_sample(sample_file, object_key, retries):
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    body = sample_file.read_bytes()

    for attempt in range(1, retries + 1):
        try:
            ensure_bucket(s3_client, bucket_name)
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=body,
                ContentType="application/x-ndjson",
            )
            return bucket_name

        except (BotoCoreError, ClientError, EndpointConnectionError):
            if attempt == retries:
                raise

            time.sleep(2)

    raise RuntimeError("Failed to upload sample file to MinIO")


def main():
    args = parse_args()
    sample_file = Path(args.file)

    if not sample_file.exists():
        raise FileNotFoundError(f"Sample file does not exist: {sample_file}")

    bucket_name = upload_sample(sample_file, args.key, args.retries)
    print(f"Uploaded {sample_file} to s3://{bucket_name}/{args.key}")


if __name__ == "__main__":
    main()

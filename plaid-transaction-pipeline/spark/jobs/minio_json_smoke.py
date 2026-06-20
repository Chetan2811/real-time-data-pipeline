import argparse
import os

from plaid_spark_common import build_spark_session, normalize_transactions


def default_input_path():
    bucket_name = os.getenv("MINIO_BUCKET", "plaid-data")
    return f"s3a://{bucket_name}/spark/raw/sample_transactions.jsonl"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read a Plaid-style JSON file from MinIO with Spark S3A."
    )
    parser.add_argument(
        "--input",
        default=default_input_path(),
        help="S3A JSON/JSONL path, for example s3a://plaid-data/spark/raw/file.jsonl.",
    )
    parser.add_argument(
        "--minio-endpoint",
        default=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        help="MinIO endpoint reachable from the Spark container.",
    )
    parser.add_argument(
        "--minio-access-key",
        default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        help="MinIO access key.",
    )
    parser.add_argument(
        "--minio-secret-key",
        default=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        help="MinIO secret key.",
    )
    parser.add_argument(
        "--show-rows",
        type=int,
        default=20,
        help="Number of normalized rows to print.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark_session(
        "PlaidMinioJsonSmoke",
        minio_config={
            "endpoint": args.minio_endpoint,
            "access_key": args.minio_access_key,
            "secret_key": args.minio_secret_key,
        },
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = spark.read.json(args.input)
        normalized_df = normalize_transactions(raw_df)

        print(f"Input path: {args.input}")
        print(f"MinIO endpoint: {args.minio_endpoint}")
        print(f"Raw row count: {raw_df.count()}")
        raw_df.printSchema()

        print("Normalized transactions from MinIO:")
        normalized_df.show(args.show_rows, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

import argparse
import os

from plaid_spark_common import build_spark_session, normalize_transactions


def default_input_path():
    bucket_name = os.getenv("MINIO_BUCKET", "plaid-data")
    return f"s3a://{bucket_name}/raw/transactions/"


def default_output_path():
    bucket_name = os.getenv("MINIO_BUCKET", "plaid-data")
    return f"s3a://{bucket_name}/processed/transactions_parquet"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Normalize Plaid-style JSON from MinIO and write Parquet."
    )
    parser.add_argument(
        "--input",
        default=default_input_path(),
        help="S3A JSON/JSONL input path.",
    )
    parser.add_argument(
        "--output",
        default=default_output_path(),
        help="S3A Parquet output directory.",
    )
    parser.add_argument(
        "--mode",
        choices=("append", "error", "errorifexists", "ignore", "overwrite"),
        default="overwrite",
        help="Spark write mode for the Parquet output.",
    )
    parser.add_argument(
        "--multi-line-json",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Read pretty-printed multi-line JSON objects. Use --no-multi-line-json for JSONL.",
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
        help="Number of written Parquet rows to print after readback.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark_session(
        "PlaidMinioJsonToParquet",
        minio_config={
            "endpoint": args.minio_endpoint,
            "access_key": args.minio_access_key,
            "secret_key": args.minio_secret_key,
        },
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = (
            spark.read.option("multiLine", str(args.multi_line_json).lower())
            .json(args.input)
        )
        normalized_df = normalize_transactions(raw_df)

        (
            normalized_df.write.mode(args.mode)
            .partitionBy("transaction_date")
            .parquet(args.output)
        )

        parquet_df = spark.read.parquet(args.output)

        print(f"Input path: {args.input}")
        print(f"Output path: {args.output}")
        print(f"Write mode: {args.mode}")
        print(f"Multi-line JSON: {args.multi_line_json}")
        print(f"Rows written: {parquet_df.count()}")
        parquet_df.printSchema()

        print("Processed Parquet rows:")
        parquet_df.orderBy("transaction_id").show(args.show_rows, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

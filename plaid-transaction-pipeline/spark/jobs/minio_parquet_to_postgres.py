import argparse
import os

from plaid_spark_common import build_spark_session


def default_input_path():
    bucket_name = os.getenv("MINIO_BUCKET", "plaid-data")
    return f"s3a://{bucket_name}/processed/transactions_parquet"


def default_postgres_url():
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "plaid_transactions")
    return f"jdbc:postgresql://{host}:{port}/{database}"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load processed Spark Parquet output from MinIO into Postgres."
    )
    parser.add_argument(
        "--input",
        default=default_input_path(),
        help="S3A Parquet input directory.",
    )
    parser.add_argument(
        "--table",
        default=os.getenv("SPARK_POSTGRES_TABLE", "transactions_spark"),
        help="Postgres table to write.",
    )
    parser.add_argument(
        "--mode",
        choices=("append", "error", "errorifexists", "ignore", "overwrite"),
        default="overwrite",
        help="Spark JDBC write mode.",
    )
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("POSTGRES_URL", default_postgres_url()),
        help="Postgres JDBC URL.",
    )
    parser.add_argument(
        "--postgres-user",
        default=os.getenv("POSTGRES_USER", "plaid_user"),
        help="Postgres username.",
    )
    parser.add_argument(
        "--postgres-password",
        default=os.getenv("POSTGRES_PASSWORD", "plaid_password"),
        help="Postgres password.",
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
        help="Number of Postgres rows to read back and print.",
    )
    return parser.parse_args()


def jdbc_properties(args):
    return {
        "user": args.postgres_user,
        "password": args.postgres_password,
        "driver": "org.postgresql.Driver",
    }


def main():
    args = parse_args()
    spark = build_spark_session(
        "PlaidMinioParquetToPostgres",
        minio_config={
            "endpoint": args.minio_endpoint,
            "access_key": args.minio_access_key,
            "secret_key": args.minio_secret_key,
        },
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        parquet_df = spark.read.parquet(args.input)

        (
            parquet_df.write.mode(args.mode)
            .option("truncate", "true")
            .jdbc(
                url=args.postgres_url,
                table=args.table,
                properties=jdbc_properties(args),
            )
        )

        loaded_df = spark.read.jdbc(
            url=args.postgres_url,
            table=args.table,
            properties=jdbc_properties(args),
        )

        print(f"Input path: {args.input}")
        print(f"Postgres URL: {args.postgres_url}")
        print(f"Postgres table: {args.table}")
        print(f"Write mode: {args.mode}")
        print(f"Rows loaded: {loaded_df.count()}")
        loaded_df.printSchema()

        print("Loaded Postgres rows:")
        loaded_df.orderBy("transaction_id").show(args.show_rows, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

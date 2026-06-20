import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, lit


def build_spark_session(app_name, minio_config=None):
    master_url = os.getenv("SPARK_MASTER_URL", "local[*]")
    builder = SparkSession.builder.appName(app_name).master(master_url)

    if minio_config:
        builder = configure_minio_s3a(builder, minio_config)

    return builder.getOrCreate()


def configure_minio_s3a(builder, config):
    endpoint = config["endpoint"]
    ssl_enabled = str(endpoint).startswith("https://")

    return (
        builder.config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", config["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )


def transaction_events_from_raw(raw_df):
    if "event" in raw_df.columns:
        return raw_df.select("event.*")

    return raw_df


def normalize_transactions(raw_df):
    event_df = transaction_events_from_raw(raw_df)

    return event_df.select(
        col("transaction_id"),
        col("account_id"),
        col("date").alias("transaction_date"),
        col("name"),
        col("merchant_name"),
        col("amount").cast("decimal(12,2)").alias("amount"),
        col("iso_currency_code"),
        concat_ws(" > ", col("category")).alias("category"),
        coalesce(col("pending"), lit(False)).alias("pending"),
        col("_plaid_change_type").alias("change_type"),
    )

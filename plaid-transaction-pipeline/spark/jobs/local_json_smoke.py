import argparse

from plaid_spark_common import build_spark_session, normalize_transactions


DEFAULT_INPUT_PATH = (
    "/opt/spark/data/spark/raw/sample_transactions.jsonl"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read a local Plaid-style JSON file with Spark."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_PATH,
        help="Local JSON/JSONL path mounted inside the Spark container.",
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
    spark = build_spark_session("PlaidLocalJsonSmoke")
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = spark.read.json(args.input)
        normalized_df = normalize_transactions(raw_df)

        print(f"Input path: {args.input}")
        print(f"Raw row count: {raw_df.count()}")
        raw_df.printSchema()

        print("Normalized transactions:")
        normalized_df.show(args.show_rows, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

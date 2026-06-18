import hashlib
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from db import get_connection
from minio_storage import (
    ensure_bucket,
    get_bucket_name,
    get_json_object,
    get_s3_client,
    list_object_keys,
    object_exists,
    put_json_object,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
CREATE_TABLES_FILE = ROOT_DIR / "sql" / "create_tables.sql"


def create_table_if_needed(connection):
    sql = CREATE_TABLES_FILE.read_text(encoding="utf-8")

    with connection.cursor() as cursor:
        cursor.execute(sql)

    connection.commit()


def clean_category(transaction):
    category = transaction.get("category")

    if isinstance(category, list):
        return " > ".join(category)

    return category or None


def normalize_transaction(transaction):
    return {
        "transaction_id": transaction.get("transaction_id"),
        "account_id": transaction.get("account_id"),
        "transaction_date": transaction.get("date"),
        "name": transaction.get("name"),
        "merchant_name": transaction.get("merchant_name"),
        "amount": transaction.get("amount"),
        "iso_currency_code": transaction.get("iso_currency_code"),
        "category": clean_category(transaction),
        "pending": transaction.get("pending", False),
    }


def insert_transaction(connection, transaction):
    row = normalize_transaction(transaction)

    if not row["transaction_id"]:
        raise ValueError("Transaction is missing transaction_id")

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO transactions (
                transaction_id,
                account_id,
                transaction_date,
                name,
                merchant_name,
                amount,
                iso_currency_code,
                category,
                pending
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO UPDATE SET
                account_id = EXCLUDED.account_id,
                transaction_date = EXCLUDED.transaction_date,
                name = EXCLUDED.name,
                merchant_name = EXCLUDED.merchant_name,
                amount = EXCLUDED.amount,
                iso_currency_code = EXCLUDED.iso_currency_code,
                category = EXCLUDED.category,
                pending = EXCLUDED.pending;
            """,
            (
                row["transaction_id"],
                row["account_id"],
                row["transaction_date"],
                row["name"],
                row["merchant_name"],
                row["amount"],
                row["iso_currency_code"],
                row["category"],
                row["pending"],
            ),
        )

    return row


def delete_transaction(connection, transaction_id):
    if not transaction_id:
        raise ValueError("Removed transaction is missing transaction_id")

    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM transactions WHERE transaction_id = %s;",
            (transaction_id,),
        )

    return {
        "transaction_id": transaction_id,
        "deleted": True,
    }


def event_from_raw_object(raw_payload):
    if isinstance(raw_payload, dict) and "event" in raw_payload:
        return raw_payload["event"]

    return raw_payload


def marker_key(raw_key):
    marker_prefix = os.getenv("MINIO_MARKER_PREFIX", "processed/_markers/").strip("/")
    digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    return f"{marker_prefix}/{digest}.json"


def processed_key(raw_key):
    raw_prefix = os.getenv("MINIO_RAW_PREFIX", "raw/transactions/").strip("/")
    processed_prefix = os.getenv(
        "MINIO_PROCESSED_PREFIX", "processed/transactions/"
    ).strip("/")

    if raw_key.startswith(f"{raw_prefix}/"):
        relative_key = raw_key[len(raw_prefix) + 1 :]
    else:
        relative_key = raw_key.rsplit("/", 1)[-1]

    return f"{processed_prefix}/{relative_key}"


def already_processed(s3_client, bucket_name, raw_key):
    return object_exists(s3_client, bucket_name, marker_key(raw_key))


def save_processed_objects(
    s3_client,
    bucket_name,
    raw_key,
    event,
    action,
    normalized_payload,
):
    processed_at = datetime.now(timezone.utc).isoformat()
    normalized_key = processed_key(raw_key)
    done_key = marker_key(raw_key)

    processed_payload = {
        "action": action,
        "transaction": normalized_payload,
        "_raw_key": raw_key,
        "_processed_at": processed_at,
    }
    marker_payload = {
        "raw_key": raw_key,
        "processed_key": normalized_key,
        "transaction_id": event.get("transaction_id"),
        "action": action,
        "processed_at": processed_at,
    }

    put_json_object(s3_client, bucket_name, normalized_key, processed_payload)
    put_json_object(s3_client, bucket_name, done_key, marker_payload)


def handle_transaction_event(connection, event):
    change_type = event.get("_plaid_change_type")

    if change_type == "removed":
        transaction_id = event.get("transaction_id")
        normalized_payload = delete_transaction(connection, transaction_id)
        return "deleted", transaction_id, normalized_payload

    normalized_payload = insert_transaction(connection, event)
    return "saved", event.get("transaction_id", "unknown"), normalized_payload


def process_raw_object(s3_client, bucket_name, connection, raw_key):
    raw_payload = get_json_object(s3_client, bucket_name, raw_key)
    event = event_from_raw_object(raw_payload)
    action, transaction_id, normalized_payload = handle_transaction_event(
        connection,
        event,
    )

    connection.commit()
    save_processed_objects(
        s3_client,
        bucket_name,
        raw_key,
        event,
        action,
        normalized_payload,
    )

    print(f"{action.title()} transaction {transaction_id} from {raw_key}")


def process_available_objects(s3_client, bucket_name, connection):
    raw_prefix = os.getenv("MINIO_RAW_PREFIX", "raw/transactions/").strip("/")
    processed_count = 0

    for raw_key in sorted(list_object_keys(s3_client, bucket_name, f"{raw_prefix}/")):
        if already_processed(s3_client, bucket_name, raw_key):
            continue

        try:
            process_raw_object(s3_client, bucket_name, connection, raw_key)
            processed_count += 1
        except Exception:
            connection.rollback()
            raise

    return processed_count


def consume_minio_to_postgres():
    load_dotenv(ROOT_DIR / ".env")

    poll_seconds = float(os.getenv("MINIO_POLL_SECONDS", "1"))
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    ensure_bucket(s3_client, bucket_name)

    raw_prefix = os.getenv("MINIO_RAW_PREFIX", "raw/transactions/")
    print(f"Watching s3://{bucket_name}/{raw_prefix}")
    print("Press Ctrl+C to stop.")

    try:
        with get_connection() as connection:
            create_table_if_needed(connection)

            while True:
                processed_count = process_available_objects(
                    s3_client,
                    bucket_name,
                    connection,
                )

                if processed_count == 0:
                    time.sleep(poll_seconds)

    except KeyboardInterrupt:
        print("Stopping MinIO to Postgres consumer...")


if __name__ == "__main__":
    consume_minio_to_postgres()

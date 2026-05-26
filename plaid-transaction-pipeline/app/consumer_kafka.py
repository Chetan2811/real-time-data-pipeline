import json
import os
from json import JSONDecodeError
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

from db import get_connection


ROOT_DIR = Path(__file__).resolve().parents[1]
CREATE_TABLES_FILE = ROOT_DIR / "sql" / "create_tables.sql"


def get_kafka_consumer():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "plaid-transaction-consumer")

    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


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

    connection.commit()


def delete_transaction(connection, transaction_id):
    if not transaction_id:
        raise ValueError("Removed transaction is missing transaction_id")

    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM transactions WHERE transaction_id = %s;",
            (transaction_id,),
        )

    connection.commit()


def decode_message(message):
    value = message.value()

    if value is None:
        raise ValueError("Kafka message has no value")

    return json.loads(value.decode("utf-8"))


def handle_kafka_error(message):
    error = message.error()

    if error.code() == KafkaError._PARTITION_EOF:
        return

    raise KafkaException(error)


def handle_transaction_event(connection, event):
    change_type = event.get("_plaid_change_type")

    if change_type == "removed":
        transaction_id = event.get("transaction_id")
        delete_transaction(connection, transaction_id)
        return "deleted", transaction_id

    insert_transaction(connection, event)
    return "saved", event.get("transaction_id", "unknown")


def consume_transactions():
    load_dotenv(ROOT_DIR / ".env")

    topic = os.getenv("KAFKA_TOPIC", "plaid_transactions_raw")
    consumer = get_kafka_consumer()
    consumer.subscribe([topic])

    print(f"Listening for Kafka topic: {topic}")
    print("Press Ctrl+C to stop.")

    try:
        with get_connection() as connection:
            create_table_if_needed(connection)

            while True:
                message = consumer.poll(1.0)

                if message is None:
                    continue

                if message.error():
                    handle_kafka_error(message)
                    continue

                try:
                    event = decode_message(message)
                    action, transaction_id = handle_transaction_event(connection, event)
                    consumer.commit(message=message, asynchronous=False)
                    print(f"{action.title()} transaction: {transaction_id}")

                except JSONDecodeError as error:
                    print(f"Skipping invalid JSON message: {error}")
                    consumer.commit(message=message, asynchronous=False)

                except Exception:
                    connection.rollback()
                    raise

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    consume_transactions()
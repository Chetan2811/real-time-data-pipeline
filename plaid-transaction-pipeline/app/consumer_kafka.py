import json
import os
import re
from datetime import datetime, timezone
from json import JSONDecodeError
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

from minio_storage import (
    ensure_bucket,
    get_bucket_name,
    get_s3_client,
    put_json_object,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
_MISSING_TOPIC_WARNINGS = set()


def get_kafka_bootstrap_servers():
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def ensure_kafka_topic(topic):
    bootstrap_servers = get_kafka_bootstrap_servers()
    partitions = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "1"))
    replication_factor = int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "1"))
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    metadata = admin_client.list_topics(timeout=10)
    if topic in metadata.topics and metadata.topics[topic].error is None:
        return

    futures = admin_client.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=partitions,
                replication_factor=replication_factor,
            )
        ]
    )

    try:
        futures[topic].result(timeout=10)
        print(
            f"Created Kafka topic {topic} "
            f"with {partitions} partition(s), replication factor {replication_factor}"
        )
    except KafkaException as error:
        kafka_error = error.args[0]

        if kafka_error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


def get_kafka_consumer():
    bootstrap_servers = get_kafka_bootstrap_servers()
    group_id = os.getenv(
        "KAFKA_TO_MINIO_GROUP_ID",
        os.getenv("KAFKA_GROUP_ID", "plaid-kafka-to-minio"),
    )

    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def decode_message(message):
    value = message.value()

    if value is None:
        raise ValueError("Kafka message has no value")

    return json.loads(value.decode("utf-8"))


def handle_kafka_error(message):
    error = message.error()

    if error.code() == KafkaError._PARTITION_EOF:
        return

    if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
        topic = message.topic()

        if topic not in _MISSING_TOPIC_WARNINGS:
            print(f"Kafka topic {topic} is not available yet. Waiting...")
            _MISSING_TOPIC_WARNINGS.add(topic)

        return

    raise KafkaException(error)


def safe_key_part(value):
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "unknown")).strip("-")
    return cleaned or "unknown"


def message_datetime(message):
    timestamp_type, timestamp_ms = message.timestamp()

    if timestamp_type > 0 and timestamp_ms is not None:
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

    return datetime.now(timezone.utc)


def raw_object_key(message, event):
    raw_prefix = os.getenv("MINIO_RAW_PREFIX", "raw/transactions/").strip("/")
    created_at = message_datetime(message)
    transaction_id = safe_key_part(event.get("transaction_id"))
    topic = safe_key_part(message.topic())

    filename = (
        f"{topic}-{message.partition()}-{message.offset()}-{transaction_id}.json"
    )

    return (
        f"{raw_prefix}/"
        f"date={created_at:%Y-%m-%d}/"
        f"hour={created_at:%H}/"
        f"{filename}"
    )


def kafka_metadata(message):
    timestamp_type, timestamp_ms = message.timestamp()
    key = message.key()

    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "key": key.decode("utf-8") if key else None,
        "timestamp_type": timestamp_type,
        "timestamp_ms": timestamp_ms,
    }


def consume_transactions_to_minio():
    load_dotenv(ROOT_DIR / ".env")

    topic = os.getenv("KAFKA_TOPIC", "plaid_transactions_raw")
    ensure_kafka_topic(topic)

    consumer = get_kafka_consumer()
    consumer.subscribe([topic])

    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    ensure_bucket(s3_client, bucket_name)

    print(f"Listening for Kafka topic: {topic}")
    print(f"Writing raw transaction events to s3://{bucket_name}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                handle_kafka_error(message)
                continue

            try:
                event = decode_message(message)
                object_key = raw_object_key(message, event)
                payload = {
                    "event": event,
                    "_kafka": kafka_metadata(message),
                    "_ingested_at": datetime.now(timezone.utc).isoformat(),
                }

                put_json_object(s3_client, bucket_name, object_key, payload)
                consumer.commit(message=message, asynchronous=False)
                print(f"Stored Kafka offset {message.offset()} at {object_key}")

            except JSONDecodeError as error:
                print(f"Skipping invalid JSON message: {error}")
                consumer.commit(message=message, asynchronous=False)

    except KeyboardInterrupt:
        print("Stopping Kafka to MinIO consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    consume_transactions_to_minio()

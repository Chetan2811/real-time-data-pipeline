import os
import time

from plaid_pipeline import (
    create_sandbox_access_token,
    fetch_transaction_updates,
    get_kafka_producer,
    get_plaid_client,
    load_environment,
    save_state,
    send_transaction_updates_to_kafka,
)


def main():
    load_environment()

    topic = os.getenv("KAFKA_TOPIC", "plaid_transactions_raw")
    webhook_url = os.getenv("PLAID_WEBHOOK_URL")

    if not webhook_url:
        raise ValueError("Missing PLAID_WEBHOOK_URL in .env")

    client = get_plaid_client()
    token_data = create_sandbox_access_token(client)
    access_token = token_data["access_token"]
    item_id = token_data["item_id"]

    print(f"Created Plaid Sandbox item: {item_id}")
    print(f"Registered webhook URL: {webhook_url}")
    print("Waiting for initial Sandbox transactions to become ready...")
    time.sleep(30)

    updates = fetch_transaction_updates(client, access_token)
    producer = get_kafka_producer()
    send_transaction_updates_to_kafka(producer, topic, updates)

    save_state(
        {
            "access_token": access_token,
            "item_id": item_id,
            "cursor": updates["next_cursor"],
        }
    )

    print("Initial sync complete.")
    print("Keep app/webhook_server.py running for real-time Plaid updates.")


if __name__ == "__main__":
    main()

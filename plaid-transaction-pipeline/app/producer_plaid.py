import json
import os
import time
from pathlib import Path

import plaid
from confluent_kafka import Producer
from dotenv import load_dotenv
from plaid.api import plaid_api
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.products import Products
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.sandbox_public_token_create_request_options import (
    SandboxPublicTokenCreateRequestOptions,
)
from plaid.model.transactions_sync_request import TransactionsSyncRequest


ROOT_DIR = Path(__file__).resolve().parents[1]


def get_plaid_client():
    load_dotenv(ROOT_DIR / ".env")

    client_id = os.getenv("PLAID_CLIENT_ID")
    secret = os.getenv("PLAID_SECRET")

    if not client_id or not secret:
        raise ValueError("Missing PLAID_CLIENT_ID or PLAID_SECRET in .env")

    configuration = plaid.Configuration(
        host=plaid.Environment.Sandbox,
        api_key={
            "clientId": client_id,
            "secret": secret,
            "plaidVersion": "2020-09-14",
        },
    )

    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


def response_to_dict(response):
    if hasattr(response, "to_dict"):
        return response.to_dict()

    return dict(response)


def create_sandbox_access_token(client):
    request = SandboxPublicTokenCreateRequest(
        institution_id=os.getenv("PLAID_INSTITUTION_ID", "ins_109508"),
        initial_products=[Products("transactions")],
        options=SandboxPublicTokenCreateRequestOptions(
            override_username=os.getenv(
                "PLAID_SANDBOX_USERNAME", "user_transactions_dynamic"
            ),
            override_password=os.getenv("PLAID_SANDBOX_PASSWORD", "pass_good"),
        ),
    )

    public_token_response = client.sandbox_public_token_create(request)
    public_token_data = response_to_dict(public_token_response)

    exchange_request = ItemPublicTokenExchangeRequest(
        public_token=public_token_data["public_token"]
    )
    exchange_response = client.item_public_token_exchange(exchange_request)
    exchange_data = response_to_dict(exchange_response)

    return exchange_data["access_token"]


def plaid_error_code(error):
    try:
        body = json.loads(error.body)
        return body.get("error_code")
    except Exception:
        return None


def fetch_transactions(client, access_token, cursor=None):
    transactions = []
    next_cursor = cursor

    while True:
        request_data = {
            "access_token": access_token,
            "count": 500,
        }

        if next_cursor:
            request_data["cursor"] = next_cursor

        request = TransactionsSyncRequest(**request_data)

        for attempt in range(1, 6):
            try:
                response = client.transactions_sync(request)
                break
            except plaid.ApiException as error:
                if plaid_error_code(error) == "PRODUCT_NOT_READY" and attempt < 5:
                    time.sleep(2)
                    continue
                raise

        response_dict = response_to_dict(response)
        transactions.extend(response_dict.get("added", []))
        transactions.extend(response_dict.get("modified", []))
        next_cursor = response_dict.get("next_cursor")

        if not response_dict.get("has_more"):
            break

    return transactions, next_cursor


def get_kafka_producer():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "plaid-transaction-producer",
        }
    )


def delivery_report(error, message):
    if error:
        print(f"Failed to send message: {error}")
    else:
        key = message.key()
        transaction_id = key.decode("utf-8") if key else "unknown"
        print(
            "Sent transaction "
            f"{transaction_id} "
            f"to {message.topic()} [{message.partition()}]"
        )


def send_transactions_to_kafka(producer, topic, transactions):
    for transaction in transactions:
        transaction_id = transaction.get("transaction_id", "")
        producer.produce(
            topic=topic,
            key=transaction_id,
            value=json.dumps(transaction, default=str),
            callback=delivery_report,
        )
        producer.poll(0)

    producer.flush()
    print(f"Sent {len(transactions)} transactions to Kafka topic: {topic}")


def main():
    load_dotenv(ROOT_DIR / ".env")

    poll_seconds = float(os.getenv("PLAID_POLL_SECONDS", "3"))
    topic = os.getenv("KAFKA_TOPIC", "plaid_transactions_raw")

    client = get_plaid_client()
    access_token = create_sandbox_access_token(client)
    producer = get_kafka_producer()
    cursor = None

    print("Created sandbox item. Waiting for transactions to become ready...")
    time.sleep(30)

    try:
        while True:
            transactions, cursor = fetch_transactions(client, access_token, cursor)

            if transactions:
                print(f"Fetched {len(transactions)} new transactions from Plaid")
                send_transactions_to_kafka(producer, topic, transactions)
            else:
                print("No new transactions from Plaid")

            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        print("Process interrupted by user. Exiting...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()

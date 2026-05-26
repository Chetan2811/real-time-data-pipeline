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
STATE_FILE = ROOT_DIR / "data" / "raw" / "plaid_state.json"


def load_environment():
    load_dotenv(ROOT_DIR / ".env")


def response_to_dict(response):
    if hasattr(response, "to_dict"):
        return response.to_dict()

    return dict(response)


def get_plaid_client():
    load_environment()

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


def create_sandbox_access_token(client):
    webhook_url = os.getenv("PLAID_WEBHOOK_URL")

    request = SandboxPublicTokenCreateRequest(
        institution_id=os.getenv("PLAID_INSTITUTION_ID", "ins_109508"),
        initial_products=[Products("transactions")],
        options=SandboxPublicTokenCreateRequestOptions(
            webhook=webhook_url,
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

    return {
        "access_token": exchange_data["access_token"],
        "item_id": exchange_data["item_id"],
    }


def plaid_error_code(error):
    try:
        body = json.loads(error.body)
        return body.get("error_code")
    except Exception:
        return None


def fetch_transaction_updates(client, access_token, cursor=None):
    added = []
    modified = []
    removed = []
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
        added.extend(response_dict.get("added", []))
        modified.extend(response_dict.get("modified", []))
        removed.extend(response_dict.get("removed", []))
        next_cursor = response_dict.get("next_cursor")

        if not response_dict.get("has_more"):
            break

    return {
        "added": added,
        "modified": modified,
        "removed": removed,
        "next_cursor": next_cursor,
    }


def load_state():
    if not STATE_FILE.exists():
        return {}

    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def update_state(**values):
    state = load_state()
    state.update(values)
    save_state(state)
    return state


def get_kafka_producer(client_id="plaid-transaction-producer"):
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
        }
    )


def delivery_report(error, message):
    if error:
        print(f"Failed to send Kafka message: {error}")
        return

    key = message.key()
    message_key = key.decode("utf-8") if key else "unknown"
    print(
        f"Sent {message_key} to "
        f"{message.topic()} [{message.partition()}] offset {message.offset()}"
    )


def send_transaction_updates_to_kafka(producer, topic, updates):
    total = 0

    for change_type in ("added", "modified"):
        for transaction in updates.get(change_type, []):
            event = dict(transaction)
            event["_plaid_change_type"] = change_type
            transaction_id = event.get("transaction_id", "")

            producer.produce(
                topic=topic,
                key=transaction_id,
                value=json.dumps(event, default=str),
                callback=delivery_report,
            )
            producer.poll(0)
            total += 1

    for removed_transaction in updates.get("removed", []):
        transaction_id = removed_transaction.get("transaction_id", "")
        event = {
            "transaction_id": transaction_id,
            "_plaid_change_type": "removed",
        }

        producer.produce(
            topic=topic,
            key=transaction_id,
            value=json.dumps(event, default=str),
            callback=delivery_report,
        )
        producer.poll(0)
        total += 1

    producer.flush()
    print(f"Sent {total} transaction events to Kafka topic: {topic}")
    return total

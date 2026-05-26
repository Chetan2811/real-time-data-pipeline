import json
import os

from flask import Flask, jsonify, request

from plaid_pipeline import (
    fetch_transaction_updates,
    get_kafka_producer,
    get_plaid_client,
    load_environment,
    load_state,
    send_transaction_updates_to_kafka,
    update_state,
)


load_environment()

app = Flask(__name__)
webhook_producer = get_kafka_producer(client_id="plaid-webhook-producer")
transaction_producer = get_kafka_producer(client_id="plaid-transaction-webhook-producer")


def send_raw_webhook_to_kafka(payload):
    topic = os.getenv("PLAID_WEBHOOK_TOPIC", "plaid_webhooks")
    item_id = payload.get("item_id", "")

    webhook_producer.produce(
        topic=topic,
        key=item_id,
        value=json.dumps(payload, default=str),
    )
    webhook_producer.flush()


def should_sync_transactions(payload):
    webhook_type = payload.get("webhook_type")
    webhook_code = payload.get("webhook_code")

    if webhook_type != "TRANSACTIONS":
        return False

    return webhook_code in {
        "SYNC_UPDATES_AVAILABLE",
        "DEFAULT_UPDATE",
        "HISTORICAL_UPDATE",
        "INITIAL_UPDATE",
        "TRANSACTIONS_REMOVED",
    }


def sync_transactions_from_plaid():
    state = load_state()
    access_token = state.get("access_token")
    cursor = state.get("cursor")

    if not access_token:
        return {
            "status": "skipped",
            "reason": "Run python app/producer_plaid.py first.",
        }

    client = get_plaid_client()
    updates = fetch_transaction_updates(client, access_token, cursor)

    transaction_topic = os.getenv("KAFKA_TOPIC", "plaid_transactions_raw")
    sent_count = send_transaction_updates_to_kafka(
        transaction_producer,
        transaction_topic,
        updates,
    )

    update_state(cursor=updates["next_cursor"])

    return {
        "status": "synced",
        "sent_to_kafka": sent_count,
        "added": len(updates["added"]),
        "modified": len(updates["modified"]),
        "removed": len(updates["removed"]),
    }


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.post("/webhook/plaid")
def plaid_webhook():
    payload = request.get_json(silent=True) or {}
    print("Plaid webhook received:", payload)

    send_raw_webhook_to_kafka(payload)

    if not should_sync_transactions(payload):
        return jsonify({"status": "ignored", "payload": payload}), 200

    result = sync_transactions_from_plaid()
    return jsonify(result), 200


if __name__ == "__main__":
    port = int(os.getenv("WEBHOOK_PORT", "5001"))
    app.run(host="0.0.0.0", port=port)

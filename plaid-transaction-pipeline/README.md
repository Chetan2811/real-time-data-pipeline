# Plaid Transaction Pipeline

This is a small Python project for learning an event-based data pipeline.

The current pipeline is webhook-driven and stores raw events in MinIO before they
are transformed and loaded:

```text
Plaid Sandbox -> producer/webhook_server.py -> Kafka -> consumer_kafka.py -> MinIO -> transform.py -> PostgreSQL
```

The setup script `producer_plaid.py` creates a Plaid Sandbox Item, registers your webhook URL, performs the first transaction sync, sends the initial transaction events to Kafka, and saves the Plaid cursor locally.

## Project Layout

```text
plaid-transaction-pipeline/
├── app/
│   ├── producer_plaid.py      # Creates Plaid Sandbox Item and does initial sync
│   ├── webhook_server.py      # Receives Plaid webhooks and syncs new updates
│   ├── consumer_kafka.py      # Reads Kafka messages and writes raw JSON to MinIO
│   ├── minio_storage.py       # S3-compatible MinIO helper code
│   ├── plaid_pipeline.py      # Shared Plaid/Kafka helper code
│   ├── db.py                  # Postgres connection helper
│   ├── transform.py           # Reads MinIO raw objects and writes to Postgres
│   └── load_postgres.py       # Older file-based script
├── data/
│   ├── raw/
│   └── processed/
├── sql/
│   └── create_tables.sql
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Create your `.env` file:

```bash
cp .env.example .env
```

Edit `.env` and add your Plaid Sandbox credentials.

For local webhooks, expose port `5001` with a public tunnel such as ngrok:

```bash
ngrok http 5001
```

Then set:

```env
PLAID_WEBHOOK_URL=https://your-ngrok-domain.ngrok-free.app/webhook/plaid
```

## Start Services

Start Postgres, Kafka, Kafka UI, and MinIO:

```bash
docker compose up -d
```

Kafka UI:

```text
http://localhost:8080
```

MinIO console:

```text
http://localhost:9001
```

Default MinIO credentials are `minioadmin` / `minioadmin`.

## Run The Real-Time Pipeline

Open terminal 1 and start the webhook server:

```bash
source .venv/bin/activate
python app/webhook_server.py
```

Open terminal 2 and start the Kafka-to-MinIO streaming consumer:

```bash
source .venv/bin/activate
python app/consumer_kafka.py
```

Open terminal 3 and start the MinIO-to-Postgres transform consumer:

```bash
source .venv/bin/activate
python app/transform.py
```

Open terminal 4 and run the Plaid setup producer:

```bash
source .venv/bin/activate
python app/producer_plaid.py
```

After setup:

1. Plaid sends transaction webhooks to `webhook_server.py`.
2. The webhook server calls Plaid `/transactions/sync`.
3. New, modified, and removed transaction events are sent to Kafka.
4. `consumer_kafka.py` writes each Kafka event to MinIO under `raw/transactions/`.
5. `transform.py` continuously polls MinIO, normalizes unprocessed raw objects, writes processed JSON under `processed/transactions/`, marks each object under `processed/_markers/`, and upserts or deletes the row in PostgreSQL.

## Check Postgres

Show a few rows:

```bash
docker compose exec postgres psql -U plaid_user -d plaid_transactions -c "SELECT * FROM transactions LIMIT 5;"
```

Count rows:

```bash
docker compose exec postgres psql -U plaid_user -d plaid_transactions -c "SELECT COUNT(*) FROM transactions;"
```

## Check MinIO

List raw objects:

```bash
docker compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker compose exec minio mc ls --recursive local/plaid-data/raw/transactions/
```

## Important Files

`data/raw/plaid_state.json` stores the local Plaid access token and cursor. It is ignored by Git.

Raw and processed transaction events now live in MinIO. `data/raw/` is still used for `plaid_state.json`.

## Notes

This project uses Plaid Sandbox only.

Run `producer_plaid.py` again only when you want to create a new Sandbox Item and reset the saved cursor.

`load_postgres.py` is the old file-based loader and is not needed for the Kafka/MinIO path.

# Plaid Transaction Pipeline

This is a small Python project for learning an event-based data pipeline.

The current pipeline is webhook-driven:

```text
Plaid Sandbox -> webhook_server.py -> Kafka -> consumer_kafka.py -> PostgreSQL
```

The setup script `producer_plaid.py` creates a Plaid Sandbox Item, registers your webhook URL, performs the first transaction sync, sends the initial transaction events to Kafka, and saves the Plaid cursor locally.

## Project Layout

```text
plaid-transaction-pipeline/
├── app/
│   ├── producer_plaid.py      # Creates Plaid Sandbox Item and does initial sync
│   ├── webhook_server.py      # Receives Plaid webhooks and syncs new updates
│   ├── consumer_kafka.py      # Reads Kafka messages and writes to Postgres
│   ├── plaid_pipeline.py      # Shared Plaid/Kafka helper code
│   ├── db.py                  # Postgres connection helper
│   ├── transform.py           # Older file-based script
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

For local webhooks, expose port `5000` with a public tunnel such as ngrok:

```bash
ngrok http 5000
```

Then set:

```env
PLAID_WEBHOOK_URL= __obtained url__
```

## Start Services

Start Postgres, Kafka, and Kafka UI:

```bash
docker compose up -d
```

Kafka UI:

```text
http://localhost:8080
```

## Run The Real-Time Pipeline

Open terminal 1 and start the webhook server:

```bash
source .venv/bin/activate
python app/webhook_server.py
```

Open terminal 2 and start the Kafka consumer:

```bash
source .venv/bin/activate
python app/consumer_kafka.py
```

Open terminal 3 and run the Plaid setup producer:

```bash
source .venv/bin/activate
python app/producer_plaid.py
```

After setup:

1. Plaid sends transaction webhooks to `webhook_server.py`.
2. The webhook server calls Plaid `/transactions/sync`.
3. New, modified, and removed transaction events are sent to Kafka.
4. The consumer writes those events to PostgreSQL.

## Check Postgres

Show a few rows:

```bash
docker compose exec postgres psql -U plaid_user -d plaid_transactions -c "SELECT * FROM transactions LIMIT 5;"
```

Count rows:

```bash
docker compose exec postgres psql -U plaid_user -d plaid_transactions -c "SELECT COUNT(*) FROM transactions;"
```

## Important Files

`data/raw/plaid_state.json` stores the local Plaid access token and cursor. It is ignored by Git.

`data/raw/` and `data/processed/` are for generated local data and are ignored by Git except for `.gitkeep` files.

## Notes

This project uses Plaid Sandbox only.

Run `producer_plaid.py` again only when you want to create a new Sandbox Item and reset the saved cursor.

The older `transform.py` and `load_postgres.py` scripts are not needed for the Kafka webhook path.

# Real-Time Plaid Transaction Pipeline

This project started as a way to learn how modern event-driven data pipelines work. Instead of pulling data on a schedule, the pipeline reacts to events as they happen using Plaid webhooks, Kafka, MinIO, and PostgreSQL.

The goal was to simulate a simplified real-world streaming architecture where financial transaction updates are captured, stored, processed, and made available for analytics.

## Architecture

```text
Plaid Sandbox
      ↓
Webhook Server
      ↓
Kafka
      ↓
Kafka Consumer
      ↓
MinIO (Raw Storage)
      ↓
Transformation Layer
      ↓
PostgreSQL
```

### Pipeline Flow

1. Plaid generates a transaction event.
2. A webhook notification is sent to the local webhook server.
3. The webhook server calls Plaid's `/transactions/sync` endpoint to retrieve changes.
4. Transaction events are published to Kafka.
5. A Kafka consumer stores the raw events in MinIO.
6. The transformation process reads raw files from MinIO, normalizes the data, and writes processed records back to MinIO.
7. The transformed records are upserted into PostgreSQL for querying and analysis.

This approach separates ingestion, storage, transformation, and serving layers, making the pipeline easier to extend and maintain.

## Project Structure

```text
plaid-transaction-pipeline/
├── app/
│   ├── producer_plaid.py
│   ├── webhook_server.py
│   ├── consumer_kafka.py
│   ├── minio_storage.py
│   ├── plaid_pipeline.py
│   ├── db.py
│   ├── transform.py
│   └── load_postgres.py
├── data/
├── sql/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

### Key Components

| File              | Purpose                                                                |
| ----------------- | ---------------------------------------------------------------------- |
| producer_plaid.py | Creates a Plaid Sandbox Item and performs the initial transaction sync |
| webhook_server.py | Receives webhook events from Plaid                                     |
| consumer_kafka.py | Consumes Kafka messages and stores raw events in MinIO                 |
| minio_storage.py  | MinIO helper functions                                                 |
| plaid_pipeline.py | Shared Plaid and Kafka utilities                                       |
| transform.py      | Reads raw objects from MinIO, transforms data, and loads PostgreSQL    |
| db.py             | PostgreSQL connection management                                       |

## Getting Started

### Create a Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Configure Environment Variables

```bash
cp .env.example .env
```

Add your Plaid Sandbox credentials and webhook URL.

For local development, expose the webhook endpoint using ngrok:

```bash
ngrok http 5001
```

Example:

```env
PLAID_WEBHOOK_URL=https://your-domain.ngrok-free.app/webhook/plaid
```

## Start Infrastructure

The project uses Docker Compose to run Kafka, PostgreSQL, MinIO, and Kafka UI.

```bash
docker compose up -d
```

### Services

| Service       | URL                   |
| ------------- | --------------------- |
| Kafka UI      | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |

Default MinIO credentials:

```text
Username: minioadmin
Password: minioadmin
```

## Running the Pipeline

### Terminal 1

Start the webhook server:

```bash
python app/webhook_server.py
```

### Terminal 2

Start the Kafka consumer:

```bash
python app/consumer_kafka.py
```

### Terminal 3

Start the transformation process:

```bash
python app/transform.py
```

### Terminal 4

Create the Plaid Sandbox Item and perform the initial sync:

```bash
python app/producer_plaid.py
```

After the initial setup, new transaction events flow through the pipeline automatically whenever Plaid sends a webhook notification.

## Notes

* This project uses Plaid Sandbox only.
* The Plaid access token and cursor are stored locally in `data/raw/plaid_state.json`.
* Run `producer_plaid.py` again only if you want to create a new Sandbox Item and reset the cursor.
* `load_postgres.py` is retained for reference but is no longer used in the Kafka → MinIO → PostgreSQL workflow.

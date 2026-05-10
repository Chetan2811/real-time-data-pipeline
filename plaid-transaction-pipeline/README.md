# Plaid Transaction Pipeline

<<<<<<< HEAD
This is a small Python data pipeline for learning how an event-based ETL flow works.

It uses Plaid Sandbox to create fake bank transactions, sends each transaction to Kafka, then reads those Kafka messages and saves them into PostgreSQL.
=======
The pipeline has three steps:
>>>>>>> 497ae34afc5fe2eb5b49be59a4fe7a524ee8e6bd

The main path is:

```text
Plaid Sandbox -> Python producer -> Kafka -> Python consumer -> PostgreSQL
```

## What Is In This Project

```text
plaid-transaction-pipeline/
├── app/
│   ├── producer_plaid.py      # Gets transactions from Plaid and sends them to Kafka
│   ├── consumer_kafka.py      # Reads Kafka messages and writes them to Postgres
│   ├── db.py                  # Postgres connection helper
│   ├── transform.py           # Older batch CSV transform script
│   └── load_postgres.py       # Older batch CSV load script
├── data/
│   ├── raw/
│   └── processed/
├── sql/
│   └── create_tables.sql      # Creates the transactions table
├── docker-compose.yml         # Postgres, Kafka, and Kafka UI
├── requirements.txt
└── README.md
```

## Setup

From the project folder:

```bash
cd plaid-transaction-pipeline
```

Create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the Python packages:

```bash
pip install -r requirements.txt
```

Create a `.env` file:

```bash
touch .env
```

Add this to `.env` and replace the Plaid values with your Sandbox credentials:

```env
PLAID_CLIENT_ID=your_plaid_client_id
PLAID_SECRET=your_plaid_sandbox_secret
PLAID_INSTITUTION_ID=ins_109508
PLAID_SANDBOX_USERNAME=user_transactions_dynamic
PLAID_SANDBOX_PASSWORD=pass_good

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=plaid_transactions_raw
KAFKA_GROUP_ID=plaid-transaction-consumer

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=plaid_transactions
POSTGRES_USER=plaid_user
POSTGRES_PASSWORD=plaid_password
```

## Start Docker Services

Start Postgres, Kafka, and Kafka UI:

```bash
docker compose up -d
```

Check that they are running:

```bash
docker compose ps
```

Kafka UI should be available here:

```text
http://localhost:8080
```

Postgres is available on:

```text
localhost:5432
```

Kafka is available to your Python code on:

```text
localhost:9092
```

## Run The Pipeline

Open one terminal and start the Kafka consumer:

```bash
python app/consumer_kafka.py
```

Leave that terminal running. It is waiting for transaction messages.

Open a second terminal, activate the same virtual environment, and run the Plaid producer:

```bash
source .venv/bin/activate
python app/producer_plaid.py
```

The producer will:

1. Create a Plaid Sandbox item.
2. Wait for transactions to become ready.
3. Fetch transactions from Plaid.
4. Send each transaction to Kafka.

The consumer will:

1. Read each Kafka message.
2. Clean up the transaction fields.
3. Insert or update the row in PostgreSQL.
4. Commit the Kafka offset after the database write succeeds.

## Check The Data

After the producer finishes, check Postgres:

```bash
docker compose exec postgres psql -U plaid_user -d plaid_transactions -c "SELECT * FROM transactions LIMIT 5;"
```

You can also count the rows:

```bash
docker compose exec postgres psql -U plaid_user -d plaid_transactions -c "SELECT COUNT(*) FROM transactions;"
```

## Optional Batch Scripts

These are from the earlier file-based version of the project:

```bash
python app/transform.py
python app/load_postgres.py
```

You do not need them for the Kafka flow.

The Kafka flow uses:

```bash
python app/producer_plaid.py
python app/consumer_kafka.py
```

## Useful Docker Commands

Stop the containers:

```bash
docker compose down
```

Restart Kafka and Kafka UI after changing `docker-compose.yml`:

```bash
docker compose up -d --force-recreate broker kafka-ui
```

View Kafka logs:

```bash
docker compose logs --tail=100 broker
```

View Kafka UI logs:

```bash
docker compose logs --tail=100 kafka-ui
```

View Postgres logs:

```bash
docker compose logs --tail=100 postgres
```

## Notes

This project uses Plaid Sandbox only. It does not connect to real bank accounts.

`app/producer_plaid.py` creates a new Plaid Sandbox item each time it runs, so running it again can create another batch of fake transactions.

The generated data files under `data/raw/` and `data/processed/` are ignored by Git.

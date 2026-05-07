# Plaid Transaction Pipeline

The pipeline has three steps:

1. Extract transactions from Plaid Sandbox into JSON.
2. Transform the JSON into a clean CSV.
3. Load the CSV into PostgreSQL.

## Project Structure

```text
plaid-transaction-pipeline/
├── app/
│   ├── extract_plaid.py
│   ├── transform.py
│   ├── load_postgres.py
│   └── db.py
├── data/
│   ├── raw/
│   └── processed/
├── sql/
│   └── create_tables.sql
├── .env.example
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## Setup

Go into the project folder:

```bash
cd plaid-transaction-pipeline
```

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

Open `.env` and add your Plaid Sandbox credentials:

```env
PLAID_CLIENT_ID=your_plaid_client_id
PLAID_SECRET=your_plaid_sandbox_secret
```

## Start PostgreSQL

```bash
docker compose up -d
```

The table is created automatically when the database starts for the first time.
If you already had the database volume, run this manually:

```bash
docker compose exec -T postgres psql -U plaid_user -d plaid_transactions < sql/create_tables.sql
```

## Run The ETL

Extract raw Plaid transactions:

```bash
python app/extract_plaid.py
```

Transform raw JSON into CSV:

```bash
python app/transform.py
```

Load CSV into PostgreSQL:

```bash
python app/load_postgres.py
```

Check the loaded data:

```bash
docker compose exec postgres psql -U plaid_user -d plaid_transactions -c "SELECT * FROM transactions LIMIT 5;"
```

## Output Files

Raw JSON:

```text
data/raw/transactions.json
```

Processed CSV:

```text
data/processed/transactions.csv
```

## Notes

This project uses Plaid Sandbox only. It creates a new sandbox Item each time
`app/extract_plaid.py` runs.

No Kafka yet.

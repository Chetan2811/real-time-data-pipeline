# How To Run

## 1. Start Services

```bash
docker compose up -d
```

## 2. Activate Python Environment

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Start Ngrok

```bash
ngrok http 5001
```

Put the ngrok URL in `.env`:

```env
PLAID_WEBHOOK_URL=https://your-ngrok-url.ngrok-free.app/webhook/plaid
```

## 4. Start The Pipeline

Open separate terminals.

Terminal 1:

```bash
python app/webhook_server.py
```

Terminal 2:

```bash
python app/consumer_kafka.py
```

Terminal 3:

```bash
python app/producer_plaid.py
```

## 5. Run Spark Processing

Write raw MinIO JSON to Parquet:

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.hadoop:hadoop-aws:3.4.2 \
  /opt/spark/jobs/minio_json_to_parquet.py
```

Load Parquet to Postgres:

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.hadoop:hadoop-aws:3.4.2,org.postgresql:postgresql:42.7.4 \
  /opt/spark/jobs/minio_parquet_to_postgres.py
```

## 6. Check Postgres

```bash
docker compose exec postgres psql -U plaid_user -d plaid_transactions -c "SELECT COUNT(*) FROM transactions_spark;"
```

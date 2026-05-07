import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]


def get_connection():
    load_dotenv(ROOT_DIR / ".env")

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "plaid_transactions"),
        user=os.getenv("POSTGRES_USER", "plaid_user"),
        password=os.getenv("POSTGRES_PASSWORD", "plaid_password"),
    )

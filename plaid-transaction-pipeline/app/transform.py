import csv
import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_FILE = ROOT_DIR / "data" / "raw" / "transactions.json"
PROCESSED_FILE = ROOT_DIR / "data" / "processed" / "transactions.csv"

COLUMNS = [
    "transaction_id",
    "account_id",
    "date",
    "name",
    "merchant_name",
    "amount",
    "iso_currency_code",
    "category",
    "pending",
]


def clean_category(transaction):
    category = transaction.get("category")

    if isinstance(category, list):
        return " > ".join(category)

    return category or ""


def normalize_transaction(transaction):
    return {
        "transaction_id": transaction.get("transaction_id", ""),
        "account_id": transaction.get("account_id", ""),
        "date": transaction.get("date", ""),
        "name": transaction.get("name", ""),
        "merchant_name": transaction.get("merchant_name", ""),
        "amount": transaction.get("amount", ""),
        "iso_currency_code": transaction.get("iso_currency_code", ""),
        "category": clean_category(transaction),
        "pending": transaction.get("pending", False),
    }


def main():
    raw_data = json.loads(RAW_FILE.read_text(encoding="utf-8"))
    transactions = raw_data.get("transactions", [])
    rows = [normalize_transaction(transaction) for transaction in transactions]

    PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)

    with PROCESSED_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} rows to {PROCESSED_FILE}")


if __name__ == "__main__":
    main()

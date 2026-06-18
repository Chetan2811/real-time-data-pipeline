
# import csv
# from pathlib import Path

# from db import get_connection


# ROOT_DIR = Path(__file__).resolve().parents[1]
# CSV_FILE = ROOT_DIR / "data" / "processed" / "transactions.csv"


# def empty_to_none(value):
#     if value == "":
#         return None
#     return value


# def to_bool(value):
#     return str(value).lower() == "true"


# def load_transactions():
#     with get_connection() as connection:
#         with connection.cursor() as cursor:
#             with CSV_FILE.open("r", newline="", encoding="utf-8") as file:
#                 reader = csv.DictReader(file)
#                 count = 0

#                 for row in reader:
#                     cursor.execute(
#                         """ """
#                         INSERT INTO transactions (
#                             transaction_id,
#                             account_id,
#                             transaction_date,
#                             name,
#                             merchant_name,
#                             amount,
#                             iso_currency_code,
#                             category,
#                             pending
#                         )
#                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#                         ON CONFLICT (transaction_id) DO UPDATE SET
#                             account_id = EXCLUDED.account_id,
#                             transaction_date = EXCLUDED.transaction_date,
#                             name = EXCLUDED.name,
#                             merchant_name = EXCLUDED.merchant_name,
#                             amount = EXCLUDED.amount,
#                             iso_currency_code = EXCLUDED.iso_currency_code,
#                             category = EXCLUDED.category,
#                             pending = EXCLUDED.pending;
#                         (
#                             row["transaction_id"],
#                             empty_to_none(row["account_id"]),
#                             empty_to_none(row["date"]),
#                             empty_to_none(row["name"]),
#                             empty_to_none(row["merchant_name"]),
#                             empty_to_none(row["amount"]),
#                             empty_to_none(row["iso_currency_code"]),
#                             empty_to_none(row["category"]),
#                             to_bool(row["pending"]),
#                         ),
#                     )
#                     count += 1

#         connection.commit()

#     print(f"Loaded {count} rows into PostgreSQL")


# if __name__ == "__main__":
#     load_transactions()


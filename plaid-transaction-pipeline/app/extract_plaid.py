# import json
# import os
# import time
# from pathlib import Path

# import plaid
# from dotenv import load_dotenv
# from plaid.api import plaid_api
# from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
# from plaid.model.products import Products
# from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
# from plaid.model.sandbox_public_token_create_request_options import (
#     SandboxPublicTokenCreateRequestOptions,
# )
# from plaid.model.transactions_sync_request import TransactionsSyncRequest


# ROOT_DIR = Path(__file__).resolve().parents[1]
# RAW_FILE = ROOT_DIR / "data" / "raw" / "transactions.json"


# def get_plaid_client():
#     load_dotenv(ROOT_DIR / ".env")

#     client_id = os.getenv("PLAID_CLIENT_ID")
#     secret = os.getenv("PLAID_SECRET")

#     if not client_id or not secret:
#         raise ValueError("Missing PLAID_CLIENT_ID or PLAID_SECRET in .env")

#     configuration = plaid.Configuration(
#         host=plaid.Environment.Sandbox,
#         api_key={
#             "clientId": client_id,
#             "secret": secret,
#             "plaidVersion": "2020-09-14",
#         },
#     )

#     api_client = plaid.ApiClient(configuration)
#     return plaid_api.PlaidApi(api_client)


# def create_sandbox_access_token(client):
#     request = SandboxPublicTokenCreateRequest(
#         institution_id=os.getenv("PLAID_INSTITUTION_ID", "ins_109508"),
#         initial_products=[Products("transactions")],
#         options=SandboxPublicTokenCreateRequestOptions(
#             override_username=os.getenv(
#                 "PLAID_SANDBOX_USERNAME", "user_transactions_dynamic"
#             ),
#             override_password=os.getenv("PLAID_SANDBOX_PASSWORD", "pass_good"),
#         ),
#     )

#     public_token_response = client.sandbox_public_token_create(request)
#     public_token_data = response_to_dict(public_token_response)
#     public_token = public_token_data["public_token"]

#     exchange_request = ItemPublicTokenExchangeRequest(public_token=public_token)
#     exchange_response = client.item_public_token_exchange(exchange_request)
#     exchange_data = response_to_dict(exchange_response)

#     return exchange_data["access_token"], exchange_data["item_id"]


# def response_to_dict(response):
#     if hasattr(response, "to_dict"):
#         return response.to_dict()

#     return dict(response)


# def plaid_error_code(error):
#     try:
#         body = json.loads(error.body)
#         return body.get("error_code")
#     except Exception:
#         return None


# def fetch_transactions(client, access_token):
#     transactions = []
#     accounts = []
#     cursor = None

#     while True:
#         request_data = {
#             "access_token": access_token,
#             "count": 500,
#         }

#         if cursor:
#             request_data["cursor"] = cursor

#         request = TransactionsSyncRequest(**request_data)

#         for attempt in range(1, 6):
#             try:
#                 response = client.transactions_sync(request)
#                 break
#             except plaid.ApiException as error:
#                 if plaid_error_code(error) == "PRODUCT_NOT_READY" and attempt < 5:
#                     time.sleep(2)
#                     continue
#                 raise

#         response_dict = response_to_dict(response)
#         transactions.extend(response_dict.get("added", []))
#         accounts = response_dict.get("accounts", accounts)
#         cursor = response_dict.get("next_cursor")

#         if not response_dict.get("has_more"):
#             break

#     return {
#         "accounts": accounts,
#         "transactions": transactions,
#         "next_cursor": cursor,
#     }


# def main():
#     client = get_plaid_client()
#     access_token, item_id = create_sandbox_access_token(client)

#     print("Created sandbox item. Waiting for transactions to become ready...")
#     time.sleep(30)

#     data = fetch_transactions(client, access_token)
#     data["item_id"] = item_id

#     RAW_FILE.parent.mkdir(parents=True, exist_ok=True)
#     RAW_FILE.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")

#     print(f"Saved {len(data['transactions'])} transactions to {RAW_FILE}")


# if __name__ == "__main__":
#     main()
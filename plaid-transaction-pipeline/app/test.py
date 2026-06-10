import boto3
import json

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

data = {
    "transaction_id": "123",
    "amount": 25.99
}

s3.put_object(
    Bucket="plaid-data",
    Key="raw/test.json",
    Body=json.dumps(data)
)

print("Uploaded")
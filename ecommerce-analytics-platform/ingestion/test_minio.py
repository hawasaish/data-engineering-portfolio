import boto3

client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

client.upload_file(
    "data/olist_orders_dataset.csv",
    "raw",
    "transactions/orders.csv"
)

print("Upload successful!")

response = client.list_buckets()

for bucket in response["Buckets"]:
    print(bucket["Name"])
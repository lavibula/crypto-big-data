from google.cloud import storage

client = storage.Client.from_service_account_json("config/btcanalysishust-495a3a227f22.json")

buckets = list(client.list_buckets())
print("Buckets:")
for bucket in buckets:
    print(bucket.name)

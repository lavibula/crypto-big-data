from kafka import KafkaConsumer
import json
import pandas as pd
from google.cloud import storage  # Lưu trữ vào Google Cloud Storage (GCS)

# Tạo Kafka consumer
consumer = KafkaConsumer(
    'crypto_prices_topic',  # Kafka topic cần lắng nghe
    bootstrap_servers=['localhost:9092'],
    group_id='crypto-group',  # Tạo group ID để quản lý consumer
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize dữ liệu JSON
)

def save_to_gcs(data, crypto_id):
    """
    Lưu dữ liệu vào Google Cloud Storage dưới dạng Parquet.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('crypto-bucket-1')  # Đổi tên bucket nếu cần
    blob = bucket.blob(f'crypto-history/{crypto_id}/{data["date"]}.parquet')
    df = pd.DataFrame(data['data'])
    df.to_parquet(blob.open('wb'))

# Lắng nghe dữ liệu từ Kafka và lưu vào GCS
for message in consumer:
    data = message.value  # Dữ liệu nhận được từ Kafka topic
    print(f"Received data for {data['crypto_id']}")
    save_to_gcs(data, data['crypto_id'])

from kafka import KafkaProducer
import json
import requests
from datetime import datetime
import time

# Tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Địa chỉ Kafka broker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Mã hóa dữ liệu thành JSON
)

def get_current_price(crypto_id):
    """
    Lấy giá của đồng tiền điện tử từ API của CoinGecko.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{crypto_id}/market_chart"
    params = {
        "vs_currency": "usd",  # Đơn vị tiền tệ là USD
        "days": "1",  # Lấy giá trong vòng 1 ngày (sẽ lấy giá theo từng phút)
        "interval": "minute",  # Mức độ tần suất là mỗi phút
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        prices = data.get("prices", [])
        # Chỉ lấy giá gần nhất
        latest_price = prices[-1] if prices else None
        if latest_price:
            return {"date": datetime.fromtimestamp(latest_price[0] / 1000).strftime("%Y-%m-%d %H:%M:%S"), "price": latest_price[1]}
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi gọi API: {e}")
        return None

def send_data_to_kafka(crypto_id, price_data):
    """
    Gửi dữ liệu giá vào Kafka.
    """
    # Gửi dữ liệu vào Kafka Topic
    producer.send('crypto_prices_topic', {'crypto_id': crypto_id, 'data': price_data})
    print(f"Sent data for {crypto_id} to Kafka: {price_data}")

# Gửi dữ liệu liên tục mỗi giây
crypto_ids = ["bitcoin", "ethereum", "dogecoin"]

while True:
    for crypto_id in crypto_ids:
        price = get_current_price(crypto_id)
        if price:
            send_data_to_kafka(crypto_id, price)  # Gửi dữ liệu ngay lập tức vào Kafka
    time.sleep(1)  # Lấy dữ liệu mỗi giây và gửi liên tục

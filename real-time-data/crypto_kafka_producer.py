from kafka import KafkaProducer
import json
import requests
from datetime import datetime
import time
import dotenv
import os
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
dotenv.load_dotenv(dotenv_path=env_path)
api=os.getenv('ccdata_api')

def get_current_price(crypto_ids : list[str]):
    """
    Fetch cryptocurrency data from the API.
    """
    url = 'https://data-api.cryptocompare.com/spot/v1/latest/tick'
    instruments=[crypto+'-USD' for crypto in crypto_ids]
    params = {
        "market": "kraken", 
        'instruments': instruments,
        "api_key": api
    }
    headers = {"Content-type": "application/json; charset=UTF-8"}
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        json_response = response.json()
        return json_response['Data']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

class SendDataKafka:
    def __init__(self, topic : str) -> None:
        self.topic=topic
        self.producer=KafkaProducer(
            bootstrap_servers=['localhost:9092'],  # Địa chỉ Kafka broker
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Mã hóa dữ liệu thành JSON
        )
    def send(self, crypto_id : str, price_data : dict):
        self.producer.send(self.topic,{'crypto_ids': crypto_id, 'data': price_data})
        print(f"Sent data for {crypto_id} to Kafka: {price_data}")
# Gửi dữ liệu liên tục mỗi phút
crypto_ids = ["BTC", "ETH",'USDT','BNB','DOGE']

send_to=SendDataKafka('realtime_date')

while True:
    prices=get_current_price(crypto_ids)
    for crypto in crypto_ids:
        send_to.send(crypto, prices[crypto+'-USD'])
    time.sleep(60)  # Lấy dữ liệu mỗi giây và gửi liên tục

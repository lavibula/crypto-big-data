import datetime
import pytz  # Import pytz for timezone handling
import requests
import json
from kafka import KafkaProducer
from time import sleep


prev_timestamp = {}

def get_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
    "ids": "bitcoin,ethereum,tether,usd-coin,ripple,cardano,dogecoin,matic-network,solana,litecoin,polkadot,shiba-inu,tron,cosmos,chainlink,stellar,near",
    "vs_currencies": "usd",
    "include_market_cap": "true",
    "include_24hr_vol": "true",
    "include_24hr_change": "true",
    "include_last_updated_at": "true"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        data = response.json()

        # Go through each coin and extract the relevant data
        prices = {}
        for coin, values in data.items():
            coin_prev_timestamp = prev_timestamp.get(coin, None)
            if coin_prev_timestamp and coin_prev_timestamp == values["last_updated_at"]:
                continue
            prev_timestamp[coin] = values["last_updated_at"]
            prices[coin] = values
        return prices
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def send_to_kafka(prices):
    vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    producer = KafkaProducer(
        bootstrap_servers='35.206.252.44:9092',
        security_protocol="PLAINTEXT",
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    producer.send('realtime-full', value=prices)
    producer.flush()  
    print(f"Data sent to Kafka at {datetime.datetime.now(tz=vietnam_tz)}")

while True:
    crypto_prices = get_crypto_prices()
    if crypto_prices:
        send_to_kafka(crypto_prices)
    sleep(30)

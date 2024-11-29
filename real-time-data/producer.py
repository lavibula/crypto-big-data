import datetime
import requests
import json
from kafka import KafkaProducer
from time import sleep

def get_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,tether,binancecoin,usd-coin,ripple,cardano,dogecoin,matic-network,solana",
        "vs_currencies": "usd"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        data = response.json()
        
        #prices = {coin: data.get(coin, {}).get('usd', 'N/A') for coin in params["ids"].split(',')}
        timestamp = datetime.datetime.utcnow().isoformat()
        prices = {
            "timestamp": timestamp,
            "prices": {coin: data.get(coin, {}).get('usd', 'N/A') for coin in params["ids"].split(',')}
        }
        
        return prices
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def send_to_kafka(prices):
    producer = KafkaProducer(
    bootstrap_servers='35.206.252.44:9092',
    security_protocol="PLAINTEXT" ,
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    
    producer.send('crypto-prices', value=prices)
    producer.flush()  
    print("Data sent to Kafka:", prices)

while True:
    crypto_prices = get_crypto_prices()
    if crypto_prices:
        send_to_kafka(crypto_prices)
    sleep(30)  
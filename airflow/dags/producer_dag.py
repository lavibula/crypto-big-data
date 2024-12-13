from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pytz
import json
from kafka import KafkaProducer
from time import sleep

# Define the function to get crypto prices
def get_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,tether,usd-coin,ripple,cardano,dogecoin,matic-network,solana,litecoin,polkadot,shiba-inu,tron,cosmos,chainlink,stellar,near",
        "vs_currencies": "usd"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        data = response.json()

        # Get the current time in UTC and convert it to Vietnam Time (UTC+7)
        vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
        vietnam_time = datetime.now(vietnam_tz)

        # Convert the datetime to ISO format
        timestamp = vietnam_time.isoformat()

        prices = {
            "timestamp": timestamp,
            "prices": {coin: data.get(coin, {}).get('usd', 'N/A') for coin in params["ids"].split(',')}
        }

        return prices
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# Define the function to send data to Kafka
def send_to_kafka():
    prices=get_crypto_prices()
    producer = KafkaProducer(
        bootstrap_servers='35.206.252.44:9092',
        security_protocol="PLAINTEXT",
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    producer.send('crypto-pricessss', value=prices)
    producer.flush()  
    print("Data sent to Kafka:", prices)

# Define the Airflow DAG
default_args = {
    'owner': 'producer',
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

with DAG(
    'crypto_price_producer',
    default_args=default_args,
    description='Fetch crypto prices and send to Kafka',
    schedule_interval='*/1 * * * *',  # This schedules the DAG to run every 1 minute
    start_date=datetime(2024, 12, 12),  # Adjust this to your desired start date
    catchup=False,
) as dag:

    send_prices_to_kafka = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka,
    )

    # Task dependencies
    send_prices_to_kafka

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.kafka.hooks.kafka import KafkaProducerHook
from airflow.operators.python_operator import PythonOperator
import requests
import json

# Hàm lấy dữ liệu từ API
def fetch_data_from_api():
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    response = requests.get(url, params={"vs_currency": "usd", "days": "1"})
    data = response.json()
    return data

# Hàm gửi dữ liệu vào Kafka
def send_to_kafka(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data')
    producer = KafkaProducerHook(bootstrap_servers='kafka:9093')
    producer.send('crypto-topic', json.dumps(data).encode('utf-8'))
    producer.flush()

# Hàm gửi dữ liệu vào PostgreSQL
def send_to_postgresql(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    for item in data['prices']:
        cursor.execute("INSERT INTO crypto_prices (timestamp, price) VALUES (%s, %s)", (item[0], item[1]))
    connection.commit()

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'catchup': False,  # Không chạy lại các tác vụ cũ
}

dag = DAG(
    'crypto_data_pipeline',
    default_args=default_args,
    description='A simple DAG to fetch crypto data and send to Kafka/PostgreSQL',
    schedule_interval=timedelta(minutes=5),  # Chạy mỗi 5 phút
)

# Task để lấy dữ liệu từ API
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_from_api,
    dag=dag,
)

# Task để gửi dữ liệu vào Kafka
send_to_kafka_task = PythonOperator(
    task_id='send_to_kafka',
    python_callable=send_to_kafka,
    provide_context=True,
    dag=dag,
)

# Task để gửi dữ liệu vào PostgreSQL
send_to_postgresql_task = PythonOperator(
    task_id='send_to_postgresql',
    python_callable=send_to_postgresql,
    provide_context=True,
    dag=dag,
)

# Xác định thứ tự các task
fetch_data_task >> [send_to_kafka_task, send_to_postgresql_task]

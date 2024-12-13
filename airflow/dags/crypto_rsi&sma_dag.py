from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 12),
}   

dag = DAG(
    'crypto_price_analysis_dag',  
    default_args=default_args,
    description='DAG to calculate RSI and SMA for cryptocurrency prices',
    schedule_interval='*/30 * * * *',  #chỗ này t think 30s 1 lần vì t đang gửi dữ liệu 30s gửi request 1 lần
)

spark_task_rsi = SparkSubmitOperator(
    task_id='spark_submit_crypto_rsi',
    application='src/streaming/crypto_price_rsi_streaming.py',  
    conn_id='spark_default',  
    conf={"spark.executor.memory": "2g", "spark.driver.memory": "1g"},
    dag=dag,
)

spark_task_sma = SparkSubmitOperator(
    task_id='spark_submit_crypto_sma',
    application='src/streaming/crypto_price_sma_streaming.py',  
    conn_id='spark_default',  
    conf={"spark.executor.memory": "2g", "spark.driver.memory": "1g"},
    dag=dag,
)


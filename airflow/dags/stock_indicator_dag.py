import sys
import os
# Add the 'src' directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import sys

# Print the directories where Python looks for modules
print("\n".join(sys.path))

from src.streaming.cryto_price_stock_streaming import to_gcs
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'owner': 'stock calculator',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['Long.NT214912@sis.hust.edu.vn'],
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    'stock_indicate_calculator',  # Tên DAG
    default_args=default_args,
    description='DAG tính chỉ số stock',
    tags=['history'],
    start_date=datetime(2024,12,1),
    schedule_interval='30 16 * * *',  
    catchup=False,  # Không chạy lại các công việc bị bỏ lỡ
)

def stock_cal_save():
    crypto_ids = ['BTC', 'ETH', 'USDT','USDC','XRP','ADA','DOGE','MATIC','SOL']
    to_gcs(crypto_ids)

calculation_task = PythonOperator(
    task_id='stock_cal_save',
    python_callable=stock_cal_save,  # Hàm sẽ được gọi
    dag=dag,
)

calculation_task 
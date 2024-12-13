from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.collect_data import main

default_args={
    'owner': 'historical data',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['Long.NT214912@sis.hust.edu.vn'],
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    'historical_data_dag',  # Tên DAG
    default_args=default_args,
    description='DAG thu thập dữ liệu lịch sử giá crypto từ CCData và lưu vào GCS',
    tags=['history'],
    start_date=datetime(2024,12,1),
    schedule_interval='0 16 * * *',  # 1:00 AM UTC+7 is 16:00 UTC
    catchup=False,  # Không chạy lại các công việc bị bỏ lỡ
)

def collect_and_save_data():
    crypto_ids = ['BTC', 'ETH', 'USDT','USDC','XRP','ADA','DOGE','MATIC','SOL', "LTC", "DOT", "SHIB", "TRX", "ATOM", "LINK", "XLM", "NEAR"]
    storage_path_gcs = "gs://crypto-historical-data-2/ver2"    
    main(crypto_ids, storage_path_gcs)

collect_data_task = PythonOperator(
    task_id='collect_and_save_data',
    python_callable=collect_and_save_data,  # Hàm sẽ được gọi
    dag=dag,
)

collect_data_task 
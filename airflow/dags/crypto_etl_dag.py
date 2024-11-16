from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from scripts.collect_data import main  # Import hàm main từ file collect_data.py

# Định nghĩa các tham số mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),  # Ngày bắt đầu DAG
    'catchup': False,  # Không chạy các công việc bị bỏ lỡ khi DAG bắt đầu
}

# Khai báo DAG
dag = DAG(
    'crypto_etl_dag',  # Tên DAG
    default_args=default_args,
    description='DAG thu thập dữ liệu lịch sử giá crypto từ CoinGecko và lưu vào GCS hoặc HDFS',
    schedule_interval='0 1 * * *',  # Lập lịch chạy vào lúc 1:00 AM mỗi ngày
    catchup=False,  # Không chạy lại các công việc bị bỏ lỡ
)

# Hàm để gọi trong PythonOperator
def collect_and_save_data():
    crypto_ids = ["bitcoin", "ethereum", "dogecoin"]
    storage_path_gcs = "gs://crypto-bucket-1/crypto-history"
    storage_path_hdfs = "./data/crypto-history"
    main(crypto_ids, storage_path_gcs, storage_path_hdfs)

# Tạo task trong Airflow để gọi hàm collect_and_save_data
collect_data_task = PythonOperator(
    task_id='collect_and_save_data',
    python_callable=collect_and_save_data,  # Hàm sẽ được gọi
    dag=dag,
)

# Task này sẽ được thực thi lúc 1:00 AM mỗi ngày, và collect_data_task sẽ được gọi

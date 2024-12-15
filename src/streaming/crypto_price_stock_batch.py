from pyspark.sql import SparkSession
from google.cloud import storage
from datetime import datetime
from pyspark.sql.functions import to_date, col
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, max as spark_max, min as spark_min, avg
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType, LongType
from pyspark.sql.window import Window
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import csv
import io
import os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
print(PROJECT_ROOT)
gcs_jar_path = os.path.join(PROJECT_ROOT, "config", "gcs-connector-hadoop3-latest.jar")
AUTH_PATH = os.path.join(PROJECT_ROOT, 'config', 'key', 'btcanalysishust-b10a2ef12088.json')
# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
  .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.gs.auth.type", "OAuth2") \
    .config("spark.hadoop.fs.gs.project.id", "btcanalysishust") \
    .config("spark.hadoop.fs.gs.input.close.input.streams.after.task.complete", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", AUTH_PATH) \
    .getOrCreate()

historical_schema = StructType([
    StructField("BASE", StringType(), True),  
    StructField("DATE", StringType(), True),
    StructField("OPEN", DoubleType(), True),
    StructField("HIGH", DoubleType(), True),
    StructField("LOW", DoubleType(), True),
    StructField("CLOSE", DoubleType(), True),
    StructField("VOLUME", DoubleType(), True),
    StructField("YEAR", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("__index_level_0__", LongType(), True)
])

def get_last_saved_date(crypto_id, storage_path : str):
    """
    Kiểm tra ngày cuối cùng đã được lưu trữ trong GCS.
    """
    bucket, dir_identify=storage_path.replace('gs://','').split('/',1)
    storage_client = storage.Client.from_service_account_json(AUTH_PATH)
    blobs = storage_client.list_blobs(bucket, prefix=f"{dir_identify}/{crypto_id}/")
    last_saved_date = None
    for blob in blobs:
        # Trích xuất ngày từ tên thư mục
        path_parts = blob.name.split('/')

        if len(path_parts) > 2: 
            year, month = path_parts[2], path_parts[3]
            date = datetime.strptime(f"{year}-{month.zfill(2)}","%Y-%m")
            if last_saved_date is None or date > last_saved_date:
                last_saved_date = date
    if  isinstance(last_saved_date, datetime):
        return last_saved_date.strftime("%Y-%m")
    else:
        return last_saved_date

def get_last_day_in_month(crypto_id, storage_path: str):
    """
    Lấy ngày cuối cùng đã được lưu trữ trong tháng gần nhất (dựa trên cột DATE trong file .parquet).
    """
    last_month = get_last_saved_date(crypto_id, storage_path)
    if last_month is None:
        return None  # Không tìm thấy dữ liệu
    # Xây dựng đường dẫn thư mục tháng gần nhất
    year, month = last_month.split('-')
    prefix = f"ver2/{crypto_id}/{year}/{month.zfill(2)}/"
    bucket, dir_identify=storage_path.replace('gs://','').split('/',1)
    storage_client = storage.Client.from_service_account_json(AUTH_PATH)
    blobs = storage_client.list_blobs(bucket, prefix=prefix)
    last_saved_day = None

    for blob in blobs:
        if blob.name.endswith('.parquet'):
            # Download và đọc file .parquet
            blob_data = blob.download_as_bytes()
            df = pd.read_parquet(io.BytesIO(blob_data))
            if 'DATE' in df.columns:
                df['DATE'] = pd.to_datetime(df['DATE'])
                max_date = df['DATE'].max()
                if last_saved_day is None or max_date > last_saved_day:
                    last_saved_day = max_date

    return last_saved_day.strftime('%Y-%m-%d') if last_saved_day else None
def get_gcs_paths(start_date_str, end_date_str, crypto_id):
    # Convert input strings to datetime objects
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    # List to hold the GCS paths
    gcs_paths = []

    # Loop through months between start_date and end_date
    current_date = start_date
    while current_date <= end_date:
        # Generate the GCS path for the current month
        path = f"gs://crypto-historical-data-2/ver2/{crypto_id}/{current_date.year}/{current_date.month:02}/data.parquet"
        gcs_paths.append(path)

        # Move to the next month
        current_date += relativedelta(months=1)

    return gcs_paths
def get_gcs_price(crypto_id : str, start_date : str , end_date : str):
    """
    Lấy dữ liệu giá từ GCS trong khoảng thời gian chỉ định và hợp nhất thành một bảng Spark.

    Args:
        crypto_id (str): Tên tài sản (crypto, cổ phiếu, v.v.).
        start_date (str): Ngày bắt đầu (định dạng 'YYYY-MM-dd').
        end_date (str): Ngày kết thúc (định dạng 'YYYY-MM-dd').

    Returns:
        DataFrame: Bảng Spark chứa tất cả dữ liệu giá hợp nhất.
    """
    # Chuyển đổi chuỗi ngày thành đối tượng datetime
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_prices=spark.read.schema(historical_schema).parquet(*get_gcs_paths(start_date,end_date,crypto_id))
    if all_prices:
        all_prices = all_prices.withColumn('DATE', to_date('DATE', 'yyyy-MM-dd'))

        # Lọc dữ liệu theo khoảng thời gian
        filtered_data = all_prices.filter(
            (col('DATE') >= start_date) & (col('DATE') <= end_date)
        )
        # Sort by the transformed date column
        sorted_data = filtered_data.orderBy('DATE', ascending= True)

        # Sort the DataFrame by the date column
        return sorted_data.select(col('BASE'),col('DATE'),col('HIGH'),col('LOW'),col('CLOSE'))
    else:
        raise ValueError("Không tìm thấy dữ liệu trong khoảng thời gian được chỉ định.")
    

class STOCK:
    def __init__(self, period_k=9, period_d=6,for_day = None, storage_path='crypto-historical-data-2/ver2' ):
        self.k=period_k
        self.d=period_d
        self.storage_path=storage_path
        self.for_day=for_day
    def get_data(self,crypto_id):
        if self.for_day:
            lastest_day=self.for_day
        else:
            lastest_day=get_last_day_in_month(crypto_id,self.storage_path)
        comeback_day=self.k+self.d-1
        start_day=datetime.strptime(lastest_day,'%Y-%m-%d')-relativedelta(days=comeback_day)
        historical_data_df=get_gcs_price(crypto_id,start_day.strftime('%Y-%m-%d'), lastest_day)
        return historical_data_df
    def calculate(self, crypto_id):
        combined_data_df=self.get_data(crypto_id)
        # Kiểm tra cột HIGH và LOW
        if not {'HIGH', 'LOW'}.issubset(combined_data_df.columns):
            raise ValueError("Dữ liệu thiếu cột HIGH hoặc LOW.")
        window_k = Window.orderBy("DATE").rowsBetween(-(self.k - 1), 0)
        combined_data_df = combined_data_df.withColumn(f"HIGH_{self.k}", spark_max("HIGH").over(window_k))
        combined_data_df = combined_data_df.withColumn(f"LOW_{self.k}", spark_min("LOW").over(window_k))

        # Tính %K
        combined_data_df = combined_data_df.withColumn(
            "%K", 
            100 * (col("CLOSE") - col(f"LOW_{self.k}")) / (col(f"HIGH_{self.k}") - col(f"LOW_{self.k}"))
        ).fillna(0, subset=["%K"])  # Thay NaN bằng 0 nếu (HIGH_k - LOW_k) = 0.
        
        # Sử dụng cửa sổ để tính trung bình động cho %D
        window_d = Window.orderBy("DATE").rowsBetween(-(self.d - 1), 0)
        combined_data_df = combined_data_df.withColumn("%D", avg("%K").over(window_d))
        # Collect all rows and access the last one
        combined_data__lastest_df = (
            combined_data_df
            .withColumn("row_num", F.row_number().over(Window.orderBy(F.col("DATE").desc())))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )
        combined_data__lastest_df.show()

        # Trả về DataFrame với %K và %D
        return combined_data__lastest_df.select("BASE","DATE","CLOSE","%K", "%D")
def process_cryto(crypto_id):
    stock = STOCK()
    df=stock.calculate(crypto_id)
    tmp_dir = f"gs://indicator-crypto/stock/batch/{crypto_id}"
    df.write \
        .format("csv") \
        .option("header", "true") \
        .option("path", tmp_dir) \
        .mode("append") \
        .save()
def to_gcs(crypto_ids):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_cryto, coin) for coin in crypto_ids]
        for future in as_completed(futures):
            print(future.result()) 

    print("Data successfully saved to GCS in separate folders for each coin.")

if __name__=='__main__':
    to_gcs(['BTC', 'ETH', 'USDT','USDC','XRP','ADA','DOGE','MATIC','SOL'])

from pyspark.sql import SparkSession
from google.cloud import storage
from datetime import datetime
from pyspark.sql.functions import to_date, col
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, max as spark_max, min as spark_min, avg
from pyspark.sql.window import Window
keyfile_path = "btcanalysishust-b10a2ef12088.json"

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
  .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.gs.auth.type", "OAuth2") \
    .config("spark.hadoop.fs.gs.project.id", "btcanalysishust") \
    .config("spark.hadoop.fs.gs.input.close.input.streams.after.task.complete", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", keyfile_path) \
    .getOrCreate()


def get_last_saved_date(crypto_id, storage_path : str):
    """
    Kiểm tra ngày cuối cùng đã được lưu trữ trong GCS hoặc HDFS.
    """
    storage_client = storage.Client.from_service_account_json("btcanalysishust-b10a2ef12088.json")
    blobs = storage_client.list_blobs(storage_path, prefix=f"ver2/{crypto_id}/")
    last_saved_date = None
    for blob in blobs:
        # Trích xuất ngày từ tên thư mục
        path_parts = blob.name.split('/')

        if len(path_parts) > 2: 
            year, month = path_parts[2], path_parts[3]
            date = datetime.strptime(f"{year}-{month.zfill(2)}","%Y-%m")
            if last_saved_date is None or date > last_saved_date:
                last_saved_date = date

    return last_saved_date.strftime("%Y-%m")

def get_gcs_price(crypto_id : str, start_date : str , end_date : str):
    """
    Lấy dữ liệu giá từ GCS trong khoảng thời gian chỉ định và hợp nhất thành một bảng Spark.

    Args:
        crypto_id (str): Tên tài sản (crypto, cổ phiếu, v.v.).
        start_date (str): Ngày bắt đầu (định dạng 'YYYY-MM').
        end_date (str): Ngày kết thúc (định dạng 'YYYY-MM').

    Returns:
        DataFrame: Bảng Spark chứa tất cả dữ liệu giá hợp nhất.
    """
    # Chuyển đổi chuỗi ngày thành đối tượng datetime
    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m")
    
    # Kiểm tra ngày bắt đầu phải nhỏ hơn hoặc bằng ngày kết thúc
    if start > end:
        raise ValueError("start_date phải nhỏ hơn hoặc bằng end_date")
    
    # Khởi tạo danh sách kết quả
    all_prices = []
    
    # Lặp qua từng tháng
    current = start.replace(day=1)  # Đặt ngày thành ngày đầu tiên của tháng
    while current <= end:
        curr_price_dir=f"gs://crypto-historical-data-2/ver2/{crypto_id}/{current.year}/{current.month:02}/data.parquet"
        all_prices.append(spark.read.parquet(curr_price_dir))
        if current.month == 12:  # Nếu là tháng 12, chuyển sang tháng 1 năm sau
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    if all_prices:
        merged_data = all_prices[0]
        for df in all_prices[1:]:
            merged_data = merged_data.union(df)
        merged_data = merged_data.withColumn('DATE', to_date('DATE', 'yyyy-MM-dd'))
        # Sort by the transformed date column
        sorted_data = merged_data.orderBy('DATE', ascending= True)

        # Sort the DataFrame by the date column
        return sorted_data.select(col('DATE'),col('HIGH'),col('LOW'),col('CLOSE'))
    else:
        raise ValueError("Không tìm thấy dữ liệu trong khoảng thời gian được chỉ định.")
    

class STOCK:
    def __init__(self, period_k=9, period_d=6, storage_path='crypto-historical-data-2' ):
        self.k=period_k
        self.d=period_d
        self.storage_path=storage_path
    def get_data(self,crypto_id):
        lastest_month=get_last_saved_date(crypto_id,self.storage_path)
        comeback_month=(self.k+self.d-1)//28+1
        start_month=datetime.strptime(lastest_month,'%Y-%m')-relativedelta(months=comeback_month)
        historical_data_df=get_gcs_price(crypto_id,start_month.strftime('%Y-%m'), lastest_month)
        # gia tri gia su
        current_close=95285.96
        timestap='2024-11-26'
        
        current_close_df = spark.createDataFrame([(current_close,current_close,current_close, timestap)], ["CLOSE",'HIGH',"LOW",'DATE'])
        combined_data_df = historical_data_df.unionByName(current_close_df,allowMissingColumns=True)
        return spark.createDataFrame(combined_data_df.tail(self.k+self.d-1))
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
        
        # Trả về DataFrame với %K và %D
        return combined_data_df.select( "%K", "%D").tail(1)[0].asDict()
    def calculate__for_all(self,crypto_ids):
        all_stocks={}
        for crypto_id in crypto_ids:
            all_stocks[crypto_id] = self.calculate(crypto_id)
        return all_stocks

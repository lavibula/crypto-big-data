from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType, LongType
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import datetime

hadoop_home = r"D:\Empty\hadoop\hadoop"
KEY_FILE = r"D:\Empty\btcanalysishust-d7c3a4830bef.json"
gcs_connector_jar = os.path.join(hadoop_home, 'share', 'hadoop', 'tools', 'lib', 'gcs-connector-hadoop3-latest.jar')

# def get_spark_session():
spark = SparkSession.builder \
    .appName("GCS Connector Test") \
    .config("spark.driver.extraClassPath", gcs_connector_jar) \
    .config("spark.executor.extraClassPath", gcs_connector_jar) \
    .config("spark.local.dir", "D:/Empty/spark-temp") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_FILE) \
    .master("local[*]") \
    .getOrCreate()
            
    
# Danh sách các cột coin
column_names = ["BTC", "ETH", "USDT", "USDC", "XRP", "ADA", "DOGE", "MATIC", "SOL"]

# Schema của dữ liệu đầu vào
schema = StructType([
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

def read_historical_data(coin):
    current_year = datetime.datetime.now().year  
    years = [str(year) for year in range(2021, current_year + 1)]  

    paths = [f"gs://crypto-historical-data-2/ver2/{coin}/{year}/*" for year in years]
    
    return (
        spark.read.schema(schema).parquet(*paths)  
        .select(F.col("DATE").cast("timestamp"), "CLOSE")
    )

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Hàm Python tính EMA
def calculate_ema_udf(period):
    alpha = 2 / (period + 1)
    
    def calculate_ema(values):
        ema_values = []
        for i, close in enumerate(values):
            if i == 0:
                ema_values.append(close)  # Giá trị EMA đầu tiên là CLOSE đầu tiên
            else:
                ema = alpha * close + (1 - alpha) * ema_values[-1]
                ema_values.append(ema)
        return round(ema_values[-1], 2)  # Trả về giá trị EMA cuối cùng trong danh sách

    return udf(lambda values: calculate_ema(values), DoubleType())

# Thêm cột EMA bằng UDF
def calculate_ema(df, column, period):
    window_spec = Window.orderBy("DATE").rowsBetween(Window.unboundedPreceding, 0)
    df = df.withColumn(f"{column}_LIST", F.collect_list(column).over(window_spec))
    ema_udf = calculate_ema_udf(period)
    return df.withColumn(f"EMA_{period}", ema_udf(F.col(f"{column}_LIST"))).drop(f"{column}_LIST")


def process_coin(coin):
    historical_data_df = read_historical_data(coin)

    # Tính toán EMA cho các khoảng thời gian
    ema_periods = [5, 10, 20, 50, 100, 200, 13, 12, 26]
    for period in ema_periods:
        historical_data_df = calculate_ema(historical_data_df, "CLOSE", period)

    historical_data_df = historical_data_df.select("DATE", "CLOSE", *[f"EMA_{period}" for period in ema_periods]).orderBy("DATE", ascending=False)


    tmp_dir = f"gs://indicator-crypto/ema_results/batch/{coin}"

    historical_data_df.write \
        .format("csv") \
        .option("header", "true") \
        .option("path", tmp_dir) \
        .mode("append") \
        .save()

    return historical_data_df

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_coin, coin) for coin in column_names]
    for future in as_completed(futures):
        print(future.result())

if __name__ == "__main__":
    coin = column_names[0]
    process_coin(coin).show()
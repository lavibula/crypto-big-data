from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType, LongType
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

gcs_jar_path = os.path.abspath("config/gcs-connector-hadoop3-latest.jar")
from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("hehee") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")\
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "config/key/btcanalysishust-495a3a227f22.json") \
    .config("spark.jars", gcs_jar_path) \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.2.0") \
    .config("spark.hadoop.fs.gs.project.id", "btcanalysishust")\
    .getOrCreate()


column_names = ["BTC", "ETH", "USDT", "USDC", "XRP", "ADA", "DOGE", "MATIC", "SOL"]

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

import datetime

def read_historical_data(coin):
    current_year = datetime.datetime.now().year  
    years = [str(year) for year in range(2021, current_year + 1)]  

    paths = [f"gs://crypto-historical-data-2/ver2/{coin}/{year}/*" for year in years]
    
    return (
        spark.read.schema(schema).parquet(*paths)  
        .select(F.col("DATE").cast("timestamp"), "CLOSE")
    )

def process_coin(coin):
    historical_data_df = read_historical_data(coin)
    window_spec = Window.orderBy("DATE").rowsBetween(Window.unboundedPreceding, 0)
    
    historical_data_df = historical_data_df.withColumn(f"SMA_5", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-4, 0)))
    historical_data_df = historical_data_df.withColumn(f"SMA_10", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-9, 0)))
    historical_data_df = historical_data_df.withColumn(f"SMA_20", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-19, 0)))
    historical_data_df = historical_data_df.withColumn(f"SMA_50", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-49, 0)))
    historical_data_df = historical_data_df.withColumn(f"SMA_100", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-99, 0)))
    historical_data_df = historical_data_df.withColumn(f"SMA_200", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-199, 0)))

    historical_data_df = historical_data_df.select("DATE", "CLOSE", "SMA_5", "SMA_10", "SMA_20", "SMA_50", "SMA_100", "SMA_200").orderBy("DATE", ascending=False)

    tmp_dir = f"gs://indicator-crypto/sma_results/batch/{coin}"

    historical_data_df.write \
        .format("csv") \
        .option("header", "true") \
        .option("path", tmp_dir) \
        .mode("append") \
        .save()

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_coin, coin) for coin in column_names]
    for future in as_completed(futures):
        print(future.result())

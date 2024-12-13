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


# Function to calculate RSI-14
def calculate_rsi(df, window_size=14):
    df = df.withColumn("price_change", F.col("CLOSE") - F.lag("CLOSE").over(Window.orderBy("DATE")))
    df = df.withColumn("gain", F.when(F.col("price_change") > 0, F.col("price_change")).otherwise(0))
    df = df.withColumn("loss", F.when(F.col("price_change") < 0, -F.col("price_change")).otherwise(0))

    window_spec = Window.orderBy("DATE").rowsBetween(-window_size + 1, 0)
    
    df = df.withColumn("avg_gain", F.avg("gain").over(window_spec))
    df = df.withColumn("avg_loss", F.avg("loss").over(window_spec))

    df = df.withColumn("rs", F.col("avg_gain") / F.col("avg_loss"))
    df = df.withColumn("RSI_14", 100 - (100 / (1 + F.col("rs"))))
    
    return df

# Function to process each coin and calculate RSI
def process_coin(coin):
    historical_data_df = read_historical_data(coin)
    combined_df = calculate_rsi(historical_data_df)

    combined_df = combined_df.select("DATE", "CLOSE", "RSI_14").orderBy("DATE", ascending=False)

    tmp_dir = f"gs://indicator-crypto/rsi_results/batch/{coin}"

    combined_df.write \
        .format("csv") \
        .option("header", "true") \
        .option("path", tmp_dir) \
        .mode("append") \
        .save()

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_coin, coin) for coin in column_names]
    for future in as_completed(futures):
        print(future.result())

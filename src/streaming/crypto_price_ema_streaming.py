from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType, LongType
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import datetime

hadoop_home = r"D:\Empty\hadoop\hadoop"
KEY_FILE = r"D:\Empty\btcanalysishust-d7c3a4830bef.json"
gcs_connector_jar = os.path.join(hadoop_home, 'share', 'hadoop', 'tools', 'lib', 'gcs-connector-hadoop3-latest.jar')

# spark.stop()
# def get_spark_session():
spark = SparkSession.builder \
    .appName("GCS Connector Test") \
    .config("spark.driver.extraClassPath", gcs_connector_jar) \
    .config("spark.executor.extraClassPath", gcs_connector_jar) \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_FILE) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3, org.postgresql:postgresql:42.7.4") \
    .master("local[*]") \
    .getOrCreate()
            
    
# Schema của dữ liệu Kafka
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("prices", StructType([
        StructField("bitcoin", FloatType()),
        StructField("ethereum", FloatType()),
        StructField("tether", FloatType()),
        StructField("usd-coin", FloatType()),
        StructField("ripple", FloatType()),
        StructField("cardano", FloatType()),
        StructField("dogecoin", FloatType()),
        StructField("matic-network", FloatType()),
        StructField("solana", FloatType()),
        StructField("litecoin", FloatType()),
        StructField("polkadot", FloatType()),
        StructField("shiba-inu", FloatType()),
        StructField("tron", FloatType()),
        StructField("cosmos", FloatType()),
        StructField("chainlink", FloatType()),
        StructField("stellar", FloatType()),
        StructField("near", FloatType()),
    ]))
])

# Đọc dữ liệu từ Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "35.206.252.44:9092") \
    .option("subscribe", "crypto-pricessss") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), schema).alias("data"))

crypto_parsed_df = parsed_df.select(
    F.to_timestamp(F.col("data.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").alias("DATE"),
    F.col("data.prices.bitcoin").alias("BTC"),
    F.col("data.prices.ethereum").alias("ETH"),
    F.col("data.prices.tether").alias("USDT"),
    F.col("data.prices.usd-coin").alias("USDC"),
    F.col("data.prices.ripple").alias("XRP"),
    F.col("data.prices.cardano").alias("ADA"),
    F.col("data.prices.dogecoin").alias("DOGE"),
    F.col("data.prices.matic-network").alias("MATIC"),
    F.col("data.prices.solana").alias("SOL")
)

# def process_batch(df, epoch_id):
#     print(f"Processing batch {epoch_id}")
#     df.show()  # Hoặc thực hiện các hành động khác

# query = crypto_parsed_df.writeStream \
#     .foreachBatch(process_batch) \
#     .start()

# query.awaitTermination()

column_names = [col for col in crypto_parsed_df.columns if col != 'DATE']

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


def read_historical_data(coin):
    return (
        spark.read.schema(historical_schema).format("parquet")
        .load(f"gs://crypto-historical-data-2/ver2/{coin}/2024/*")
        .select(F.col("DATE").cast("timestamp"), "CLOSE")
    )
    
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
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

def process_coin(coin, micro_batch_latest_df):
    historical_data_df = read_historical_data(coin)
    # print(coin)
    # print("historical_data_df")
    # historical_data_df.show(truncate=False)
    micro_batch = micro_batch_latest_df.select("DATE", coin).withColumnRenamed(coin, "CLOSE")
    # print("micro_batch")
    # micro_batch.show(truncate=False)
    # Kết hợp dữ liệu hiện tại với dữ liệu lịch sử
    combined_df = micro_batch.unionByName(historical_data_df)
    # print("combined_df")
    # combined_df.show()
    
    # Tính toán EMA
    ema_periods = [5, 10, 20, 50, 100, 200, 12, 13, 26]
    for period in ema_periods:
        combined_df = calculate_ema(combined_df, "CLOSE", period)
    
    # Sắp xếp và lọc dữ liệu theo ngày
    combined_df = combined_df.select("DATE", "CLOSE", *[f"EMA_{period}" for period in ema_periods]).orderBy("DATE", ascending=False)
    
    current_date = F.current_date() 
    combined_df = combined_df.filter(F.col("DATE") >= current_date)
    # combined_df.show()
    
    # Check if combined_df has any data
    if combined_df.count() > 0:
        # Lưu kết quả vào GCS nếu có dữ liệu
        tmp_dir = f"gs://indicator-crypto/ema_results/tmp/{coin}"
        combined_df.write \
            .format("csv") \
            .option("header", "true") \
            .option("path", tmp_dir) \
            .mode("append") \
            .save()
        print(f"{coin} wrote")
    else:
        print(f"No data for coin: {coin} to write")


def process_batch(micro_batch_df, batch_id):
    micro_batch_latest_df = (
        micro_batch_df
        .withColumn("row_num", F.row_number().over(Window.orderBy(F.col("DATE").desc())))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    
    # Sử dụng ThreadPoolExecutor để xử lý nhiều đồng tiền song song
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_coin, coin, micro_batch_latest_df) for coin in column_names]
        for future in as_completed(futures):
            print(future.result())
    # for coin in column_names:
    #     process_coin(coin, micro_batch_latest_df)
    #     # result.show()
    #     break
    
# Chạy job streaming
query = crypto_parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()

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

def process_coin(coin, micro_batch_latest_df):
    historical_data_df = read_historical_data(coin)
    micro_batch = micro_batch_latest_df.select("DATE", coin).withColumnRenamed(coin, "CLOSE")
    combined_df = micro_batch.unionByName(historical_data_df)

    window_spec = Window.orderBy("DATE").rowsBetween(Window.unboundedPreceding, 0)
    combined_df = combined_df.withColumn(f"SMA_5", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-4, 0)))
    combined_df = combined_df.withColumn(f"SMA_10", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-9, 0)))
    combined_df = combined_df.withColumn(f"SMA_20", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-19, 0)))
    combined_df = combined_df.withColumn(f"SMA_50", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-49, 0)))
    combined_df = combined_df.withColumn(f"SMA_100", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-99, 0)))
    combined_df = combined_df.withColumn(f"SMA_200", F.avg(F.col("CLOSE")).over(window_spec.rowsBetween(-199, 0)))

    combined_df = combined_df.orderBy("DATE", ascending=False)
    
    current_date = F.current_date() 
    combined_df = combined_df.filter(F.col("DATE") >= current_date)

    tmp_dir = f"gs://indicator-crypto/sma_results/tmp/{coin}"

    #combined_df.write.format("console").option("truncate", False).save()
    combined_df.write \
        .format("csv") \
        .option("header", "true") \
        .option("path", tmp_dir) \
        .mode("append") \
        .save()
    
def process_batch(micro_batch_df, batch_id):
    micro_batch_latest_df = (
        micro_batch_df
        .withColumn("row_num", F.row_number().over(Window.orderBy(F.col("DATE").desc())))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_coin, coin, micro_batch_latest_df) for coin in column_names]
        for future in as_completed(futures):
            print(future.result()) 

# Thực thi stream
query = crypto_parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
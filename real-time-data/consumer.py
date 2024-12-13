from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName(":))") \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("bitcoin", FloatType()),
    StructField("ethereum", FloatType()),
    StructField("tether", FloatType()),
    StructField("binancecoin", FloatType()),
    StructField("usd-coin", FloatType()),
    StructField("ripple", FloatType()),
    StructField("cardano", FloatType()),
    StructField("dogecoin", FloatType()),
    StructField("matic-network", FloatType()),
    StructField("solana", FloatType())
])

crypto_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "35.206.252.44:9092") \
    .option("subscribe", "crypto-pricessss") \
    .load()

crypto_json_df = crypto_df.selectExpr("CAST(value AS STRING)")


crypto_parsed_df = crypto_json_df.select(from_json(col("value"), schema).alias("data"))

final_df = crypto_parsed_df.select(
    col("data.timestamp").alias("timestamp"),
    col("data.bitcoin").alias("bitcoin"),
    col("data.ethereum").alias("ethereum"),
    col("data.tether").alias("tether"),
    col("data.binancecoin").alias("binancecoin"),
    col("data.usd-coin").alias("usd_coin"),
    col("data.ripple").alias("ripple"),
    col("data.cardano").alias("cardano"),
    col("data.dogecoin").alias("dogecoin"),
    col("data.matic-network").alias("matic_network"),
    col("data.solana").alias("solana")
)

final_df.writeStream.format("console").option("truncate", False).save()

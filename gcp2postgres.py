hadoopFilesPath = r"C:\Projects\crypto-big-data\.venv\Lib\site-packages\pyspark\hadoop"
import os
os.environ["HADOOP_HOME"] = hadoopFilesPath
os.environ["hadoop.home.dir"] = hadoopFilesPath
os.environ["PATH"] = os.environ["PATH"] + f";{hadoopFilesPath}\\bin"
os.environ["PYSPARK_PYTHON"] = "python"

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType, LongType
from pyspark import SparkContext
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

gcs_jar_path = os.path.abspath("config/gcs-connector-hadoop3-latest.jar")

spark = SparkSession.builder \
    .appName("bull") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")\
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "config/btcanalysishust-f1f296ff752d.json") \
    .config("spark.jars", gcs_jar_path) \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.2.0") \
    .config("spark.hadoop.fs.gs.project.id", "btcanalysishust")\
    .config("spark.executor.heartbeatInterval","3600s")\
    .config("spark.network.timeout","4000s")\
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
        spark.read.schema(schema).parquet(*paths).drop("__index_level_0__")
    )

# Uppercase all columns
def uppercase_all_columns(df):
    for column in df.columns:
        df = df.withColumnRenamed(column, column.upper())
    return df

# Reset data
import psycopg2
conn = psycopg2.connect(
    host="34.80.252.31",
    database="combined",
    user="nmt",
    password="nmt_acc"
)
cur = conn.cursor()
cur.execute("DELETE FROM bigdata.price")
conn.commit()
cur.close()
conn.close()

for coin in column_names:
    df = read_historical_data(coin)
    df = uppercase_all_columns(df)
    # Push to Postgres
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://34.80.252.31:5432/combined") \
        .option("dbtable", "bigdata.price") \
        .option("user", "nmt") \
        .option("password", "nmt_acc") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
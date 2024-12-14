hadoopFilesPath = r"G:\20232\intern ai cmc\big data\crypto-big-data\venv\Lib\site-packages\pyspark\hadoop"
import os
os.environ["HADOOP_HOME"] = hadoopFilesPath
os.environ["hadoop.home.dir"] = hadoopFilesPath
os.environ["PATH"] = os.environ["PATH"] + f";{hadoopFilesPath}\\bin"
os.environ["PYSPARK_PYTHON"] = "python"
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType, LongType
import os
PROJECT_ROOT = os.path.dirname(__file__)
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
column_names = ["BTC", "ETH", "USDT", "USDC", "XRP", "ADA", "DOGE", "MATIC", "SOL"]
schema = StructType([
    StructField("DATE", StringType(), True),
    StructField("CLOSE", DoubleType(), True),
    StructField("%K", DoubleType(), True),
    StructField("%D", DoubleType(), True),
])

import datetime

def read_data(crypto_id):
    path=f"gs://indicator-crypto/stock/batch/{crypto_id}/*"
    return (
        spark.read.schema(schema).parquet(path)
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
cur.execute("DELETE FROM bigdata.stock")
conn.commit()
cur.close()
conn.close()

for coin in column_names:
    df = read_data(coin)
    df = uppercase_all_columns(df)
    # Push to Postgres
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://34.80.252.31:5432/combined") \
        .option("dbtable", "bigdata.stock") \
        .option("user", "nmt") \
        .option("password", "nmt_acc") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
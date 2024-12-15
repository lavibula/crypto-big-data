from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType, LongType
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import datetime

hadoop_home = r"D:\Empty\hadoop\hadoop"
KEY_FILE = r"D:\Empty\btcanalysishust-d7c3a4830bef.json"
gcs_connector_jar = os.path.join(hadoop_home, 'share', 'hadoop', 'tools', 'lib', 'gcs-connector-hadoop3-latest.jar')
postgresql_jar = os.path.join(hadoop_home, 'share', 'hadoop', 'tools', 'lib', 'postgresql-42.7.4.jar')

# from pyspark import SparkContext 
# sc = SparkContext.getOrCreate() 
# spark.stop()
# def get_spark_session():
# spark = SparkSession.builder \
#     .appName("GCS Connector Test") \
#     .config("spark.driver.extraClassPath", gcs_connector_jar + "," + postgresql_jar) \
#     .config("spark.executor.extraClassPath", gcs_connector_jar + "," + postgresql_jar) \
#     .config("spark.local.dir", "D:/Empty/spark-temp") \
#     .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_FILE) \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3") \
#     .master("local[*]") \
#     .getOrCreate()
spark = SparkSession.builder \
    .appName("bull") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")\
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", KEY_FILE) \
    .config("spark.jars", f"{gcs_connector_jar},{postgresql_jar}") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.hadoop.fs.gs.project.id", "btcanalysishust")\
    .config("spark.executor.heartbeatInterval","3600s")\
    .config("spark.network.timeout","4000s")\
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
        .withColumn("DATE", F.col("DATE").cast("timestamp"))
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
        
    # Thêm cột MACD = EMA12 - EMA26
    historical_data_df = historical_data_df.withColumn("MACD", 
        F.col("EMA_12") - F.col("EMA_26")
    )
    # historical_data_df = historical_data_df.select("DATE", "CLOSE", *[f"EMA_{period}" for period in ema_periods]).orderBy("DATE", ascending=False)
    
    # Thêm cột BASE để phân biệt dữ liệu
    historical_data_df = historical_data_df.withColumn("BASE", F.lit(coin))

    # Lựa chọn và sắp xếp các cột theo schema của bảng `bigdata.ema`
    historical_data_df = historical_data_df.select(
        "BASE",
        F.date_format("DATE", "yyyy-MM-dd").alias("DATE"),
        "OPEN",  
        "CLOSE",
        "HIGH", 
        "LOW",  
        "VOLUME", 
        F.year("DATE").alias("YEAR"),
        F.month("DATE").alias("MONTH"),
        *[F.col(f"EMA_{period}").alias(f"ema{period}") for period in ema_periods],
        "MACD"
    ).orderBy("DATE", ascending=False)
    historical_data_df.printSchema()
    tmp_dir = f"gs://indicator-crypto/ema_results/batch/{coin}"
    historical_data_df.write \
        .format("csv") \
        .option("header", "true") \
        .option("path", tmp_dir) \
        .mode("overwrite") \
        .save()
    
    # historical_data_df.write \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://34.80.252.31:5432/combined") \
    #     .option("dbtable", "bigdata.ema") \
    #     .option("user", "nmt") \
    #     .option("password", "nmt_acc") \
    #     .option("driver", "org.postgresql.Driver") \
    #     .mode("append") \
    #     .save()
    try:
        # Print first few rows to verify data
        print("Sample data:")
        # historical_data_df.show(5)

        # Attempt to write with more verbose error handling
        # historical_data_df.write \
        # .format("jdbc") \
        # .option("url", "jdbc:postgresql://localhost:5432/testttt") \
        # .option("dbtable", "ema") \
        # .option("user", "postgres") \
        # .option("password", "sehilnlf") \
        # .option("driver", "org.postgresql.Driver") \
        # .mode("append") \
        # .save()
        historical_data_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://34.80.252.31:5432/combined") \
            .option("dbtable", "bigdata.ema") \
            .option("user", "nmt") \
            .option("password", "nmt_acc") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        print(f"Successfully wrote data for {coin}")
    except Exception as e:
        print(f"Error writing data for {coin}: {e}")
        # Optionally, log the full traceback
        import traceback
        traceback.print_exc()

    return historical_data_df
    # historical_data_df.write \
    # .format("jdbc") \
    # .option("url", "jdbc:postgresql://localhost:5432/testttt") \
    # .option("dbtable", "ema") \
    # .option("user", "postgres") \
    # .option("password", "sehilnlf") \
    # .option("driver", "org.postgresql.Driver") \
    # .mode("append") \
    # .save()

    return historical_data_df

# with ThreadPoolExecutor() as executor:
#     futures = [executor.submit(process_coin, coin) for coin in column_names]
#     for future in as_completed(futures):
#         print(future.result())

coin = column_names[0]
process_coin(coin).show()
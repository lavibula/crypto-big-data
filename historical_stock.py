from src.streaming.cryto_price_stock_streaming import STOCK,get_gcs_price,get_last_day_in_month
from pyspark.sql import SparkSession
from google.cloud import storage
from datetime import datetime
from pyspark.sql.functions import to_date, col
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, max as spark_max, min as spark_min, avg
from pyspark.sql.window import Window
import pandas as pd
import csv
import io
import os

class STOCK:
    def __init__(self, period_k=9, period_d=6, storage_path='crypto-historical-data-2/ver2' ):
        self.k=period_k
        self.d=period_d
        self.storage_path=storage_path
    def get_data(self,crypto_id):
        lastest_day=get_last_day_in_month(crypto_id,self.storage_path)
        historical_data_df=get_gcs_price(crypto_id,'2021-01-01', lastest_day)
        return historical_data_df
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
        combined_data_df.show(n=20)
        # Trả về DataFrame với %K và %D
        return combined_data_df.select( "DATE","CLOSE","%K", "%D")
    def calculate__for_all(self,crypto_ids):
        all_stocks={}
        for crypto_id in crypto_ids:
            data = self.calculate(crypto_id)
            data.write \
                .format("csv") \
                .option("header", "true") \
                .option("path", f"gs://indicator-crypto/stock/batch/{crypto_id}") \
                .mode("append") \
                .save()
        return all_stocks
if __name__=='__main__':
    calculator=STOCK()
    df=calculator.calculate__for_all(['BTC', 'ETH', 'USDT','USDC','XRP','ADA','DOGE','MATIC','SOL'])

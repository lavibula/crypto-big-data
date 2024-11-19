import requests
from datetime import datetime, timedelta
import pandas as pd
import math
from google.cloud import storage
from storage import save_to_gcs_parquet, save_to_hdfs_parquet  # Các hàm lưu vào GCS hoặc HDFS
from metal_crawl import fetch_yahoo_data

def get_data_api(crypto_id, end_date, num_days):
    """
    Fetch cryptocurrency data from the API.
    """
    url = 'https://data-api.cryptocompare.com/spot/v1/historical/days'
    end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
    params = {
        "market": "kraken", 
        'instrument': crypto_id + '-USD',
        'to_ts': end_timestamp,
        'limit': num_days, 
        "api_key": "f007c27012ef526c4a0216b612c9b7f68e4a02430e08925284d2e7b613daa0e2"
    }
    headers = {"Content-type": "application/json; charset=UTF-8"}
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        json_response = response.json()
        data = json_response['Data']
        df = pd.DataFrame(data)[['BASE', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]
        df = df.iloc[::-1].reset_index(drop=True)
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

def get_historical_prices(crypto_id, start_date, end_date):
    num_days = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days
    num_api_calls = math.ceil(num_days / 2000)
    curr_lastest_date = end_date
    
    # Initialize an empty DataFrame to store all data
    full_df = pd.DataFrame(columns=['BASE', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])
    
    for i in range(num_api_calls):
        limit = (datetime.strptime(curr_lastest_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days
        curr_df = get_data_api(crypto_id, curr_lastest_date, limit if limit < 2000 else 2000)
        
        if curr_df is not None:
            # Concatenate the new data to the existing full_df
            full_df = pd.concat([full_df, curr_df], ignore_index=True)
        
            # Find the oldest timestamp and adjust the current lastest date
            oldest_timestamp = curr_df['TIMESTAMP'].min()
            oldest_datetime = datetime.fromtimestamp(oldest_timestamp)
            curr_lastest_date = (oldest_datetime - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            break  # Exit if there's an error fetching the data
    full_df['TIMESTAMP'] = full_df['TIMESTAMP'].apply(formated_date)
    full_df.rename(columns={'TIMESTAMP': 'DATE'}, inplace=True)
    full_df['DATE'] = pd.to_datetime(full_df['DATE'])
    return full_df
def formated_date(timestap):
    return datetime.fromtimestamp(timestap).strftime('%Y-%m-%d')
def partition_data_by_date(df : pd.DataFrame):
    """
    Phân vùng dữ liệu theo năm, tháng, ngày.
    """
    
    df['date'] = pd.to_datetime(df['DATE'])
    df['year'] = df['DATE'].dt.year
    df['month'] = df['DATE'].dt.month
    df['day'] = df['DATE'].dt.day
    return df


def save_data(df : pd.DataFrame, crypto_id, storage_path, save_method):
    """
    Lưu dữ liệu vào GCS hoặc HDFS dưới dạng Parquet và phân vùng theo coin, year, month, day.
    """
    df_partitioned = partition_data_by_date(df)
    for _, row in df_partitioned.iterrows():
        year, month, day = row['year'], row['month'], row['day']
        path = f"{storage_path}/{crypto_id}/{year}/{month}/{day}/data.parquet"
        save_method(df_partitioned, path)  # Gọi phương thức lưu trữ tùy thuộc vào `save_method`


def get_last_saved_date(crypto_id, storage_path):
    """
    Kiểm tra ngày cuối cùng đã được lưu trữ trong GCS hoặc HDFS.
    """
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(storage_path, prefix=f"{crypto_id}/")
    
    last_saved_date = None
    for blob in blobs:
        # Trích xuất ngày từ tên thư mục
        path_parts = blob.name.split('/')
        if len(path_parts) > 3:  # {crypto_id}/{year}/{month}/{day}/data.parquet
            year, month, day = path_parts[1], path_parts[2], path_parts[3]
            date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            if last_saved_date is None or date > last_saved_date:
                last_saved_date = date
    
    return last_saved_date


def main(crypto_ids : list[str] = None,mental_ids : list[str] = None,  storage_path_gcs : str = None, storage_path_hdfs : str = None):
    """
    Quy trình lấy dữ liệu và lưu trữ.
    """
    for crypto_id in crypto_ids:

        # Lấy ngày cuối cùng đã lưu trên GCS hoặc HDFS
        last_saved_date = get_last_saved_date(crypto_id, storage_path_gcs)
        
        # Nếu không có dữ liệu đã lưu, bắt đầu từ ngày ngày 01/01/2017
        if last_saved_date is None:
            start_date='2017-01-01'
        else:
            # Nếu có dữ liệu đã lưu, lấy dữ liệu từ ngày hôm sau của ngày cuối đã lưu
            start_date = (datetime.strptime(last_saved_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        # Lấy ngày hôm nay để làm endpoint kết thúc
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        # Lấy dữ liệu từ API
        historical_prices = get_historical_prices(crypto_id, start_date, end_date)

        if historical_prices is not None and not historical_prices.empty:
            print(f"Dữ liệu mới của {crypto_id} từ {start_date} đến {end_date}:")
            print(historical_prices)

            # Lưu dữ liệu vào GCS dưới dạng Parquet
            if not storage_path_gcs:
                save_data(historical_prices, crypto_id, storage_path_gcs, save_to_gcs_parquet)
            # Hoặc bạn có thể lưu vào HDFS
            if not storage_path_hdfs:
                save_data(historical_prices, crypto_id, storage_path_hdfs, save_to_hdfs_parquet)



if __name__ == "__main__":
    # Các đồng tiền cần lấy dữ liệu
    crypto_ids = ["BTC", "ETH"]
    storage_path_gcs = "gs://cryto-historical-data"
    storage_path_hdfs = "./data/crypto-history"

    main(crypto_ids, True,storage_path_gcs )

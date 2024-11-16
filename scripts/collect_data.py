import requests
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import storage
from scripts import save_to_gcs_parquet, save_to_hdfs  # Các hàm lưu vào GCS hoặc HDFS


def get_historical_prices(crypto_id, start_date, end_date):
    """
    Lấy giá của đồng tiền điện tử từ API của CoinGecko từ start_date đến end_date.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{crypto_id}/market_chart/range"
    start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

    params = {
        "vs_currency": "usd",  # Đơn vị tiền tệ là USD
        "from": start_timestamp,
        "to": end_timestamp
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        prices = data.get("prices", [])
        formatted_data = [{"date": datetime.fromtimestamp(price[0] / 1000).strftime("%Y-%m-%d"), "price": price[1]} for price in prices]
        return pd.DataFrame(formatted_data)
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi gọi API: {e}")
        return None


def partition_data_by_date(df):
    """
    Phân vùng dữ liệu theo năm, tháng, ngày.
    """
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    return df


def save_data(df, crypto_id, storage_path, save_method):
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


def main(crypto_ids, storage_path_gcs, storage_path_hdfs):
    """
    Quy trình lấy dữ liệu và lưu trữ.
    """
    for crypto_id in crypto_ids:
        # Lấy ngày cuối cùng đã lưu trên GCS hoặc HDFS
        last_saved_date = get_last_saved_date(crypto_id, storage_path_gcs)
        
        # Nếu không có dữ liệu đã lưu, bắt đầu từ ngày hôm qua
        if last_saved_date is None:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
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
            save_data(historical_prices, crypto_id, storage_path_gcs, save_to_gcs_parquet)
            # Hoặc bạn có thể lưu vào HDFS
            # save_data(historical_prices, crypto_id, storage_path_hdfs, save_to_hdfs_parquet)



if __name__ == "__main__":
    # Các đồng tiền cần lấy dữ liệu
    crypto_ids = ["bitcoin", "ethereum", "dogecoin"]
    storage_path_gcs = "gs://crypto-bucket-1/crypto-history"
    storage_path_hdfs = "./data/crypto-history"

    main(crypto_ids, storage_path_gcs, storage_path_hdfs)

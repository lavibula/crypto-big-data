from collect_data import get_last_saved_date
from google.cloud import storage
import pandas as pd
import io

def get_last_day_in_month(crypto_id, storage_path: str):
    """
    Lấy ngày cuối cùng đã được lưu trữ trong tháng gần nhất (dựa trên cột DATE trong file .parquet).
    """
    last_month = get_last_saved_date(crypto_id, storage_path)
    if last_month is None:
        return None  # Không tìm thấy dữ liệu
    # Xây dựng đường dẫn thư mục tháng gần nhất
    year, month = last_month.split('-')
    prefix = f"ver2/{crypto_id}/{year}/{month.zfill(2)}/"
    bucket, dir_identify=storage_path.replace('gs://','').split('/',1)
    storage_client = storage.Client.from_service_account_json("config/btcanalysishust-b10a2ef12088.json")
    blobs = storage_client.list_blobs(bucket, prefix=prefix)
    last_saved_day = None

    for blob in blobs:
        if blob.name.endswith('.parquet'):
            # Download và đọc file .parquet
            blob_data = blob.download_as_bytes()
            df = pd.read_parquet(io.BytesIO(blob_data))
            if 'DATE' in df.columns:
                df['DATE'] = pd.to_datetime(df['DATE'])
                max_date = df['DATE'].max()
                if last_saved_day is None or max_date > last_saved_day:
                    last_saved_day = max_date

    return last_saved_day.strftime('%Y-%m-%d') if last_saved_day else None


if __name__=='__main__':
    crypto_id = "ETH"
    storage_path = "gs://crypto-historical-data-2/ver2"
    last_day = get_last_day_in_month(crypto_id, storage_path)
    print(f"Last saved day for {crypto_id}: {last_day}")

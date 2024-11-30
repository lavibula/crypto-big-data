from google.cloud import storage
import pandas as pd
import os
import tempfile

def save_to_gcs_parquet(df : pd.DataFrame, gcs_path : str):
    """
    Lưu dữ liệu DataFrame dưới dạng Parquet vào Google Cloud Storage.
    """
    # Tạo tệp Parquet tạm thời
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        local_parquet_path = tmp_file.name  # Lấy đường dẫn tệp tạm thời
        # Lưu dữ liệu DataFrame dưới dạng Parquet vào tệp tạm thời
        df.to_parquet(local_parquet_path, engine='pyarrow')

    # Tải tệp lên GCS
    storage_client = storage.Client.from_service_account_json("./config/btcanalysishust-b10a2ef12088.json")
    bucket_name, blob_name = gcs_path.replace('gs://', '').split('/', 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Tải tệp từ hệ thống cục bộ lên GCS
    blob.upload_from_filename(local_parquet_path)
    
    # Xóa tệp cục bộ sau khi tải lên
    os.remove(local_parquet_path)
    
    print(f"Lưu dữ liệu vào GCS thành công tại: {gcs_path}")
import pyarrow as pa
import pyarrow.parquet as pq


def save_to_hdfs_parquet(df, hdfs_path):
    """
    Lưu dữ liệu DataFrame dưới dạng Parquet vào HDFS.
    """
    table = pa.Table.from_pandas(df)
    pq.write_table(table, hdfs_path)
    print(f"Lưu dữ liệu vào HDFS thành công tại: {hdfs_path}")

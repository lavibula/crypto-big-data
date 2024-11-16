from google.cloud import storage
import pandas as pd


def save_to_gcs_parquet(df, gcs_path):
    """
    Lưu dữ liệu DataFrame dưới dạng Parquet vào Google Cloud Storage.
    """
    # Lưu dữ liệu dưới dạng Parquet vào GCS
    df.to_parquet(gcs_path, engine='pyarrow')
    
    # Tải lên GCS
    storage_client = storage.Client()
    bucket_name, blob_name = gcs_path.replace('gs://', '').split('/', 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(gcs_path)
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

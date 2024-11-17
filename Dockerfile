# Sử dụng image Python cơ bản
FROM python:3.8-slim

# Đặt thư mục làm việc trong container
WORKDIR /app

# Sao chép các file Python vào thư mục làm việc
COPY . /app

# Cài đặt các thư viện cần thiết (bao gồm google-cloud-storage, kafka-python, v.v.)
RUN pip install --no-cache-dir -r requirements.txt

# Lệnh mặc định để chạy script Python
CMD ["python", "crypto_kafka_consumer.py"]

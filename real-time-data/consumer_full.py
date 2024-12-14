from kafka import KafkaConsumer
import datetime
import pytz
import json

consumer = KafkaConsumer(
    'realtime-full',
    bootstrap_servers='35.206.252.44:9092',
    security_protocol="PLAINTEXT",
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
)

def format_message(message):
    coin_infos = []
    for coin, values in message.items():
        info = {
            "coin": coin,
            "price": values["usd"],
            "market_cap": values["usd_market_cap"],
            "volume_24h": values["usd_24h_vol"],
            "change_24h": values["usd_24h_change"],
            "updated_at": values["last_updated_at"]
        }
        coin_infos.append(info)
    return coin_infos

def send2postgres(coin_info: dict):
    import psycopg2
    table = "bigdata.price_24h"
    conn = psycopg2.connect(
        host="34.80.252.31",
        database="combined",
        user="nmt",
        password="nmt_acc"
    )
    cur = conn.cursor()
    # Get number of rows
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE BASE = '{coin_info['coin']}'")
    # If table size is 2880 (number of 30 second intervals in 24 hours), drop the oldest row
    if cur.fetchone()[0] == 2880:
        cur.execute(f"DELETE FROM {table} ORDER BY UPDATED_AT LIMIT 1 ASC WHERE BASE = '{coin_info['coin']}'")
    # Check if key already exists
    # This is a bug that has not been examined, but for now, just check for duplicates and ignore them
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE BASE = '{coin_info['coin']}' AND UPDATED_AT = TO_TIMESTAMP({coin_info['updated_at']})")
    if cur.fetchone()[0] > 0:
        return
    # Insert new row
    cur.execute(f"""
                INSERT INTO bigdata.price_24h (BASE, PRICE, MARKET_CAP, VOLUME_24H, CHANGE_24H, UPDATED_AT)
                VALUES ('{coin_info["coin"]}', {coin_info["price"]}, {coin_info["market_cap"]}, {coin_info["volume_24h"]}, {coin_info["change_24h"]}, TO_TIMESTAMP({coin_info["updated_at"]}));
                """)
    conn.commit()
    cur.close()
    conn.close()

tz = pytz.timezone("Asia/Ho_Chi_Minh")
for message in consumer:
    coin_infos = format_message(message.value)
    for coin_info in coin_infos:
        send2postgres(coin_info)
    print(f"Data inserted at {datetime.datetime.now(tz=tz)}")
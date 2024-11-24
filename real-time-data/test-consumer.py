from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'test_topic', 
    bootstrap_servers='35.206.252.44:9092',
    auto_offset_reset='earliest',  
    enable_auto_commit=True,
    group_id='my-group'
)

print("Receiving messages from Kafka...")
for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")

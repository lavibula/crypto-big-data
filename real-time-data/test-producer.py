from kafka import KafkaProducer
import time


producer = KafkaProducer(
    bootstrap_servers='35.206.252.44:9092',
    security_protocol="PLAINTEXT" 
)


topic = 'test_topic'

print("Sending messages to Kafka...")
for i in range(1, 11):
    message = f"Message {i}"
    producer.send(topic, value=message.encode('utf-8'))
    print(f"Sent: {message}")
    time.sleep(1)

producer.close()

from kafka import KafkaConsumer, KafkaProducer
import json
import time

KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'emoji_highest_topic'
OUTPUT_TOPIC = 'cluster_topic'

# Kafka consumer for the highest emoji topic
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

# Kafka producer for the cluster topic
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Main Publisher is running...")

# Listen to the highest emoji topic and forward to the cluster topic
for message in consumer:
    data = message.value
    print(f"Received from Spark: {data}")
    
    # Send data to the cluster topic
    producer.send(OUTPUT_TOPIC, data)
    print(f"Forwarded to Cluster: {data}")
    
    # Introduce a delay of 1 second
    time.sleep(1)

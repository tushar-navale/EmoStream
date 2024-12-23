from kafka import KafkaConsumer, KafkaProducer
import json

KAFKA_BROKER = 'localhost:9092'
CLUSTER_TOPIC = 'cluster_topic'
NEW_CLUSTER_TOPIC = 'cluster_1'

# Kafka consumer for the cluster topic
consumer = KafkaConsumer(
    CLUSTER_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

# Kafka producer for the new cluster topic
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Cluster Publisher is running...")

# Distribute messages to the new topic
for message in consumer:
    data = message.value
    print(f"Broadcasting to Subscribers: {data}")
    
    # Forward the message to the new cluster topic
    producer.send(NEW_CLUSTER_TOPIC, data)
    print(f"Forwarded to {NEW_CLUSTER_TOPIC}: {data}")

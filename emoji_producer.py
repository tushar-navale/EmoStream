from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
from concurrent.futures import ThreadPoolExecutor
import logging

app = Flask(__name__)

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'emoji_topic'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configure Kafka producer for better performance (batching, compression, etc.)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500,  # Batch messages before sending to Kafka
    batch_size=65536,  # 64KB batch size for better throughput
    compression_type='gzip',  # Use compression to save bandwidth
    acks='all'  # Wait for all brokers to acknowledge (stronger consistency)
)

# Use a thread pool for handling requests concurrently
executor = ThreadPoolExecutor(max_workers=200)  # Adjust for high concurrency

def send_to_kafka(message):
    # Send the message without flushing every time
    producer.send(KAFKA_TOPIC, message)

@app.route('/send-emoji', methods=['POST'])
def send_emoji():
    data = request.get_json()
    if not data or 'user_id' not in data or 'emoji_type' not in data or 'timestamp' not in data:
        return jsonify({"error": "Invalid input"}), 400

    # Log received request data
    logging.info(f"Received data: {data}")

    message = {
        'user_id': data['user_id'],
        'emoji_type': data['emoji_type'],
        'timestamp': data['timestamp']
    }
    # Submit task to thread pool for non-blocking behavior
    executor.submit(send_to_kafka, message)
    return jsonify({"message": "Emoji data sent successfully!"}), 200

if __name__ == '__main__':
    # Run with Gunicorn for better performance in production environments
    app.run(debug=True, host='0.0.0.0', port=5000)

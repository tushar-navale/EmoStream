from flask import Flask, jsonify, request
from kafka import KafkaConsumer
import json
import threading

# Flask app setup
app = Flask(__name__)

# Kafka setup
KAFKA_BROKER = 'localhost:9092'
CLUSTER_TOPIC = 'cluster_1'

consumer = KafkaConsumer(
    CLUSTER_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

# Store the latest updates and registered clients
latest_updates = []
registered_clients = set()

# Route to serve the updates
@app.route('/updates', methods=['GET'])
def get_updates():
    return jsonify(latest_updates)

# Route to register a client
@app.route('/register', methods=['POST'])
def register_client():
    client_id = request.json.get('client_id')
    if client_id:
        registered_clients.add(client_id)
        print(f"Client registered: {client_id}")
        return jsonify({'status': 'registered', 'client_id': client_id}), 200
    return jsonify({'error': 'Invalid client_id'}), 400

# Route to deregister a client
@app.route('/deregister', methods=['POST'])
def deregister_client():
    client_id = request.json.get('client_id')
    if client_id and client_id in registered_clients:
        registered_clients.remove(client_id)
        print(f"Client deregistered: {client_id}")
        return jsonify({'status': 'deregistered', 'client_id': client_id}), 200
    return jsonify({'error': 'Client not registered'}), 400

# Function to listen to Kafka and store updates
def listen_to_kafka():
    global latest_updates
    print("Subscriber is listening to Kafka...")
    for message in consumer:
        data = message.value

        
        emoji_type = data.get('emoji_type')
        scaled_count = data.get('scaled_count', 0)
        
        # Print emoji scaled_count times
        for _ in range(scaled_count):
            print(emoji_type)
        
        latest_updates.append(data)
        # Keep only the last 10 updates (optional)
        latest_updates = latest_updates[-10:]

# Run the Flask app
if __name__ == '__main__':
    # Run Kafka listener in a separate thread
    kafka_thread = threading.Thread(target=listen_to_kafka, daemon=True)
    kafka_thread.start()
    # Run Flask server
    app.run(host='0.0.0.0', port=5010)
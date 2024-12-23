import requests
import time
import uuid
import signal
import sys

# Flask server URL
SERVER_URL = "http://localhost:5010"
client_id = str(uuid.uuid4())  # Unique ID for this client

# Register the client
def register_client():
    try:
        response = requests.post(f"{SERVER_URL}/register", json={'client_id': client_id})
        if response.status_code == 200:
            print(f"Client registered successfully: {client_id}")
        else:
            print(f"Failed to register client: {response.json()}")
    except Exception as e:
        print(f"Error registering client: {e}")

# Deregister the client
def deregister_client():
    try:
        response = requests.post(f"{SERVER_URL}/deregister", json={'client_id': client_id})
        if response.status_code == 200:
            print(f"Client deregistered successfully: {client_id}")
        else:
            print(f"Failed to deregister client: {response.json()}")
    except Exception as e:
        print(f"Error deregistering client: {e}")

# Fetch updates from the server
def fetch_updates():
    try:
        response = requests.get(f"{SERVER_URL}/updates")
        if response.status_code == 200:
            updates = response.json()

            for update in updates:
                emoji_type = update.get('emoji_type')
                scaled_count = update.get('scaled_count', 0)
                
                # Print emoji scaled_count times
                for _ in range(scaled_count):
                    print(emoji_type)  # Prints emoji scaled_count times
        else:
            print(f"Failed to fetch updates. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error connecting to the server: {e}")

# Handle graceful shutdown
def handle_exit(signum, frame):
    print("Shutting down client...")
    deregister_client()
    sys.exit(0)

if __name__ == '__main__':  # Fixed condition to check for main script execution
    # Handle SIGINT and SIGTERM for graceful shutdown
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    print("Client is running...")
    register_client()

    try:
        while True:
            fetch_updates()
            time.sleep(1)  # Poll every 5 seconds
    except KeyboardInterrupt:
        handle_exit(None, None)

import requests
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor

# Set up API endpoint
url = "http://localhost:5000/send-emoji"

# Generate random emoji data
def generate_emoji_data():
    user_id = 3 # Random user ID
    emoji_type = random.choice(["😊", "😢", "🔥", "❤️"])  # Random emoji
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
    return {
        "user_id": user_id,
        "emoji_type": emoji_type,
        "timestamp": timestamp
    }

# Function to send a single emoji request
def send_emoji():
    data = generate_emoji_data()
    try:
        response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(data))
        if response.status_code == 200:
            print("Emoji sent:", data)
        else:
            print("Failed to send emoji:", response.status_code, response.text)
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

# Main loop to send emojis in parallel
if __name__ == "__main__":
    # Use a thread pool for concurrency
    with ThreadPoolExecutor(max_workers=500) as executor:
        while True:
            # Submit 500 requests in parallel every second
            for _ in range(500):  # 500 requests per second
                executor.submit(send_emoji)
            time.sleep(2)  # Repeat every second

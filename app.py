from flask import Flask, render_template
from kafka import KafkaConsumer
import json

app = Flask(__name__)

# Kafka consumer setup
consumer = KafkaConsumer(
    'wikimedia_recentchange',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='webapp-group',
    value_deserializer=lambda x: x.decode('utf-8')  # Decoding without JSON conversion initially
)

@app.route('/')
def index():
    messages = []
    try:
        for message in consumer:
            print(f"Raw message: {message.value}")  # Print raw message for debugging
            try:
                json_data = json.loads(message.value)
                messages.append(json_data)
                print(f"Valid JSON message added: {json_data}")
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e} - Skipping this message.")
            if len(messages) >= 10:  # Display 10 messages at a time
                break
    except Exception as e:
        print(f"Error occurred: {e}")
    
    return render_template('index.html', messages=messages)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)

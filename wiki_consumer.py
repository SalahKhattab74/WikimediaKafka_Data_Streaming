from kafka import KafkaConsumer
import json  # Make sure to import the json module

# Define the Kafka consumer
consumer = KafkaConsumer(
    'wikimedia_recentchange',  # Topic name
    bootstrap_servers='localhost:9092',  # Kafka broker
    auto_offset_reset='earliest',  # Start reading at the earliest message if no offset is set
    enable_auto_commit=True,  # Automatically commit offsets
    group_id='wikimedia-group',  # Consumer group id
)

if __name__ == "__main__":
    try:
        for message in consumer:
            try:
                raw_message = message.value.decode('utf-8')  # Decode the raw bytes to a string
                print(f"Raw message: {raw_message}")
                
                # Attempt to load the message as JSON
                json_data = json.loads(raw_message)
                print(f"Received JSON message: {json_data}")
                
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e} - Raw message: {raw_message}")
            except Exception as e:
                print(f"Unexpected error: {e} - Raw message: {raw_message}")
    except KeyboardInterrupt:
        print("Consumer script interrupted by user.")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        consumer.close()

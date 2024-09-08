from kafka import KafkaProducer
import json
import requests

# Define the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# URL for Wikimedia's Recent Changes Stream
url = 'https://stream.wikimedia.org/v2/stream/recentchange'

def produce_messages():
    response = requests.get(url, stream=True)
    for line in response.iter_lines():
        if line:
            try:
                # Convert the line to a string and check if it starts with "data:"
                line_str = line.decode('utf-8')
                if line_str.startswith('data:'):
                    # Extract JSON data after "data: " and send it to Kafka
                    json_data = json.loads(line_str[6:])
                    producer.send('wikimedia_recentchange', value=json_data)
                    print(f"Sent data: {json_data}")
                else:
                    print(f"Ignored non-data line: {line_str}")
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e} - Problematic line: {line_str}")
            except Exception as e:
                print(f"Unexpected error: {e} - Problematic line: {line_str}")
        else:
            print("Received empty line.")

if __name__ == "__main__":
    try:
        produce_messages()
    except KeyboardInterrupt:
        print("Producer script interrupted by user.")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        producer.close()

from confluent_kafka import Consumer, KafkaException, KafkaError # type: ignore
import json

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9094',  # Replace with your Kafka broker address
    'group.id': 'python-consumer-group',   # Consumer group ID
    'auto.offset.reset': 'earliest',       # Start reading at the earliest message
}

# Initialize the Kafka consumer
consumer = Consumer(kafka_config)

# Subscribe to the topic
topic = "spark_write_stream"  # Replace with your Kafka topic name
consumer.subscribe([topic])

print(f"Listening to Kafka topic: {topic}")

try:
    while True:
        # Poll for a message
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue  # No new messages; continue polling
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Successfully received a message
            print(f"Received message: {msg.value().decode('utf-8')}")
            
            # Process the JSON message
            try:
                json_data = json.loads(msg.value().decode('utf-8'))
                print(f"Processed JSON: {json_data}")
            except json.JSONDecodeError:
                print("Failed to decode JSON message")

except KeyboardInterrupt:
    print("Consumer interrupted by user")

finally:
    # Close the consumer on exit
    consumer.close()
    print("Kafka consumer closed")

import json
import time
import sys
import random
from confluent_kafka import Producer # type: ignore

# Callback for delivery report
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def crate_and_run_kafka_topic(topic_name):

        # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9094',  # Replace with your Kafka broker address
        'client.id': 'python-producer',
    }

    # Initialize the Kafka producer
    producer = Producer(kafka_config)

    # Kafka topic name
    topic = topic_name # Replace with your Kafka topic name

    i = 0

    try:
        while True:


            json_messages = [
            {"id": i, "age": random.randint(1,90)},
            {"id": (i+1), "age": random.randint(1,90)},
            {"id": (i+2), "age": random.randint(1,90)},
            ]

            i += 3
            
            print(json_messages)
        # Produce messages
            for message in json_messages:
                # Serialize the JSON object to a string
                message_json = json.dumps(message)
                
                # Send the message to the Kafka topic
                producer.produce(
                    topic,
                    key=str(message["id"]),  # Optional: Use 'id' as the message key
                    value=message_json,
                    callback=delivery_report,
                )
                time.sleep(1)

                print("Message sent to kafka topic:", topic)

    except KeyboardInterrupt:
        return "producer interrupted by user"

    finally:
        # Close the consumer on exit
        producer.flush()
        return "Kafka producer closed"

if __name__ == "__main__":

    topic_name = sys.argv[1]

    crate_and_run_kafka_topic(topic_name)

#py '.\kafka producer.py' kafka_1
#py '.\kafka producer.py' kafka_2
#py '.\kafka producer.py' kafka_3
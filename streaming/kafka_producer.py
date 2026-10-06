import sys
import os
import json
import time
from confluent_kafka import Producer

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from producer.generator import generate_order
from dotenv import load_dotenv

load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "orders"

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Delivered message to {msg.topic()} [{msg.partition()}]")

def run_producer(num_records=5, delay=1.0):
    print(f"Starting Kafka Producer, sending to {KAFKA_BROKER} topic '{TOPIC_NAME}'...")
    
    # Configure Kafka Producer
    conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(conf)

    for i in range(num_records):
        # Generate an order
        order = generate_order(order_id_start=1001 + i)
        print(f"Sending order {order['order_id']}")
        
        # Asynchronously produce a message. The delivery report callback will
        # be triggered from poll() below, or flush() once the message has
        # been successfully delivered or failed permanently.
        producer.produce(
            topic=TOPIC_NAME, 
            key=str(order['order_id']),
            value=json.dumps(order), 
            callback=delivery_report
        )
        
        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)
        
        # Simulate real-time delay
        time.sleep(delay)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    print("Flushing records...")
    producer.flush()
    print("Producer finished.")

if __name__ == "__main__":
    # In a real environment, this might run infinitely: while True: run_producer(...)
    run_producer(num_records=5, delay=1.0)

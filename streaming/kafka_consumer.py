import json
from confluent_kafka import Consumer, KafkaError

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "orders"
GROUP_ID = "streamflow_consumer_group"

def run_consumer():
    print(f"Starting Kafka Consumer, listening to {KAFKA_BROKER} topic '{TOPIC_NAME}'...")
    
    # Configure Kafka Consumer
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest' # Start reading from the beginning if no offset is found
    }
    
    consumer = Consumer(conf)
    
    # Subscribe to the topic
    consumer.subscribe([TOPIC_NAME])

    try:
        while True:
            # Poll for new messages (wait up to 1 second)
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event (not a real error)
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            # Parse and display the message
            order_data = json.loads(msg.value().decode('utf-8'))
            print(f"Received Order {order_data['order_id']} for customer {order_data['customer_id']} ({order_data['product']})")

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        print("Consumer shut down cleanly.")

if __name__ == "__main__":
    run_consumer()

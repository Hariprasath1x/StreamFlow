import json
import time
from confluent_kafka import Producer

def run():
    p = Producer({'bootstrap.servers': 'localhost:9092'})
    topic = "orders"

    # Test C: Malformed JSON
    p.produce(topic, key="err", value="not json {")

    # Test D: Negative quantity
    t_d = {"order_id": 2001, "quantity": -5, "unit_price": 100.0, "timestamp": "2026-10-06T12:00:00Z"}
    p.produce(topic, key="2001", value=json.dumps(t_d))

    # Test E: Negative unit price
    t_e = {"order_id": 2002, "quantity": 1, "unit_price": -10.0, "timestamp": "2026-10-06T12:00:00Z"}
    p.produce(topic, key="2002", value=json.dumps(t_e))

    # Test F: Missing order_id
    t_f = {"quantity": 1, "unit_price": 10.0, "timestamp": "2026-10-06T12:00:00Z"}
    p.produce(topic, key="2003", value=json.dumps(t_f))

    # Test G: Invalid timestamp
    t_g = {"order_id": 2004, "quantity": 1, "unit_price": 10.0, "timestamp": "invalid_date"}
    p.produce(topic, key="2004", value=json.dumps(t_g))

    p.flush()
    print("Produced invalid test records.")

if __name__ == "__main__":
    run()

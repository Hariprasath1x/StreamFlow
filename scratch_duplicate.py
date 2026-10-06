import json
from confluent_kafka import Producer

def run():
    p = Producer({'bootstrap.servers': 'localhost:9092'})
    order = {
        "order_id": 1001,
        "customer_id": 45,
        "product": "Keyboard",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": 899.0,
        "city": "Mumbai",
        "timestamp": "2026-10-06T12:00:00Z"
    }
    p.produce("orders", key=str(order['order_id']), value=json.dumps(order))
    p.produce("orders", key=str(order['order_id']), value=json.dumps(order))
    p.flush()
    print("Sent duplicate orders.")

if __name__ == "__main__":
    run()

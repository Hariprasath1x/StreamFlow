import json
import csv
import random
import os
from datetime import datetime, timezone
from faker import Faker

fake = Faker('en_IN')

CATEGORIES = {
    "Electronics": [("Wireless Mouse", 499.0), ("Keyboard", 899.0), ("Headphones", 1499.0)],
    "Clothing": [("T-Shirt", 299.0), ("Jeans", 999.0), ("Jacket", 1999.0)],
    "Home": [("Coffee Mug", 150.0), ("Table Lamp", 750.0), ("Cushion Cover", 300.0)]
}

def generate_order(order_id_start=1001):
    category = random.choice(list(CATEGORIES.keys()))
    product, unit_price = random.choice(CATEGORIES[category])
    
    return {
        "order_id": order_id_start,
        "customer_id": random.randint(1, 100),
        "product": product,
        "category": category,
        "quantity": random.randint(1, 5),
        "unit_price": unit_price,
        "city": fake.city(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def generate_dataset(num_records=100, output_format="json", output_dir="data/sample"):
    os.makedirs(output_dir, exist_ok=True)
    records = []
    
    for i in range(num_records):
        records.append(generate_order(order_id_start=1001 + i))
        
    if output_format == "json":
        filepath = os.path.join(output_dir, "orders.json")
        with open(filepath, 'w') as f:
            json.dump(records, f, indent=4)
        print(f"Generated {num_records} JSON records at {filepath}")
        
    elif output_format == "csv":
        filepath = os.path.join(output_dir, "orders.csv")
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
        print(f"Generated {num_records} CSV records at {filepath}")

if __name__ == "__main__":
    generate_dataset(num_records=10, output_format="json")
    generate_dataset(num_records=10, output_format="csv")

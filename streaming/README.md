# Streaming Component (Kafka)

## What is Apache Kafka?
Apache Kafka is a distributed event streaming platform used to handle real-time data feeds. It allows systems to quickly and reliably send and receive messages asynchronously. 

## Key Kafka Concepts
- **Kafka Broker:** A single Kafka server. It receives messages from producers, assigns them offsets, and commits them to storage on disk.
- **Topic:** A named logical channel where messages are published. (e.g., `orders`). Think of it as a category or a folder for events.
- **Producer:** An application that writes (publishes) data to Kafka topics. In this project, `producer.py` creates e-commerce orders and sends them to the `orders` topic.
- **Consumer:** An application that reads (subscribes to) data from Kafka topics. In this project, `consumer.py` listens to the `orders` topic and displays the incoming orders.

## Why Kafka?
In real-world data engineering, batch processing (running a job once a day) isn't fast enough for use cases like fraud detection or real-time analytics. Kafka enables a **streaming architecture** where events are processed the moment they occur.

## Why use Kafka instead of calling the consumer directly?
If the Python generator directly called a consumer function (e.g., `process_order()`), the system would be tightly coupled. 
1. If the consumer crashes, the producer crashes. 
2. If the producer is faster than the consumer, memory will overflow.
Kafka acts as a durable shock absorber (a message queue/buffer). The producer writes at its own speed, the consumer reads at its own speed, and the data is safely persisted to disk.

## Architecture Flow
```text
Python Generator 
       ↓ 
 Kafka Producer 
       ↓ 
 `orders` topic 
       ↓ 
 Kafka Consumer
```

## How to Run

1. Start Kafka via Docker:
   ```bash
   docker compose up -d
   ```
2. Run the Consumer (in one terminal):
   ```bash
   .venv/bin/python streaming/consumer.py
   ```
3. Run the Producer (in another terminal):
   ```bash
   .venv/bin/python streaming/producer.py
   ```

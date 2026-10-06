# StreamFlow: Real-Time E-Commerce Data Pipeline

## 1. Project Overview
StreamFlow is a Data Engineering portfolio project demonstrating an end-to-end streaming data pipeline. It simulates a modern e-commerce platform where orders are generated in real-time, streamed through a message broker, processed via distributed computing, and loaded into an analytical data warehouse. 

*Note: This is a student-scale project designed to run locally to demonstrate core Data Engineering fundamentals, not a production-scale deployment.*

## 2. Business Problem
Batch processing overnight is no longer sufficient for modern e-commerce. Businesses need to know about inventory drops, revenue spikes, or fraudulent patterns *as they happen*. StreamFlow solves this by implementing a real-time streaming architecture that captures, cleans, and stores order data instantly.

## 3. Architecture

```text
                  [ Real-Time Streaming Pipeline ]

  Python Generator        Apache Kafka          PySpark           PostgreSQL         Power BI
 (Source System)   ->   (Event Broker)  ->   (Processing)  ->  (Data Warehouse) ->  (Dashboard)
        │                     │                    │                   │                  │
 Generates orders      Topics: `orders`     Validates & Cleans    Analytical SQL    Visualizes KPIs
  (JSON events)                            Calculates Revenue       Storage
  
  
                  [ Batch Orchestration ]
                  
  Apache Airflow
  (Orchestrator)
        │
 Manages daily jobs,
 historical loads,
 and dependencies
```

## 4. Technology Stack
- **Python (3.12):** Core language for scripts and application logic.
- **Apache Kafka:** Distributed event streaming platform used to decouple the source systems from the analytics processing.
- **Apache Spark (PySpark):** Distributed computing framework used (via Structured Streaming) to process the real-time Kafka data stream.
- **PostgreSQL:** Relational database serving as the analytical data warehouse.
- **Apache Airflow:** Workflow orchestrator used to schedule and monitor batch data pipelines.
- **Docker & Docker Compose:** Containerization tools used to easily run Kafka and PostgreSQL infrastructure locally.
- **Power BI:** Business intelligence tool for data visualization (documented connection).

## 5. Data Flow
1. **Ingestion:** `kafka_producer.py` continuously generates synthetic e-commerce JSON orders and pushes them to the Kafka `orders` topic.
2. **Streaming:** Kafka holds the messages durably, acting as a shock absorber.
3. **Processing:** `spark_streaming.py` subscribes to the Kafka topic. It parses the JSON, drops invalid records (e.g. negative quantities), extracts time features, and calculates the `total_amount` for the order.
4. **Storage:** The PySpark job uses `foreachBatch` to micro-batch the cleaned data into the `processed_orders` table in PostgreSQL. Bad data goes to `invalid_orders`.
5. **Analytics:** Power BI connects to PostgreSQL to display revenue and order metrics.

## 6. How to Run

1. **Start Infrastructure (Kafka & Postgres):**
   ```bash
   docker compose up -d
   ```
2. **Install Dependencies (Requires Python 3.12+):**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Configure Environment:**
   ```bash
   cp .env.example .env
   ```
4. **Start the PySpark Streaming Job:**
   *(Runs continuously listening for data)*
   ```bash
   .venv/bin/python streaming/spark_streaming.py
   ```
5. **Start the Order Generator (in a new terminal):**
   *(Generates events into Kafka)*
   ```bash
   source .venv/bin/activate
   .venv/bin/python streaming/kafka_producer.py
   ```
6. **Teardown:**
   ```bash
   docker compose down -v
   ```

## 7. Example Output

**Kafka Event (Raw JSON):**
```json
{"order_id": 1001, "customer_id": 45, "product": "Keyboard", "category": "Electronics", "quantity": 2, "unit_price": 899.0, "city": "Mumbai", "timestamp": "2026-10-06T12:00:00Z"}
```

**PostgreSQL (Processed Data):**
| order_id | customer_id | product  | quantity | unit_price | total_amount | order_date | order_hour |
|----------|-------------|----------|----------|------------|--------------|------------|------------|
| 1001     | 45          | Keyboard | 2        | 899.00     | 1798.00      | 2026-10-06 | 12         |

## 8. Key Data Engineering Concepts Demonstrated
- **Real-Time Streaming Ingestion** using Kafka Producers and Consumers.
- **Micro-batch Processing** via PySpark Structured Streaming.
- **ETL (Extract, Transform, Load)** logic applied in-flight.
- **Data Quality & Validation** (Dead Letter Queue pattern for invalid records).
- **Spark Checkpointing** to maintain streaming progress and offsets across restarts.
- **SQL Data Modeling** and Warehousing concepts.
- **Workflow Orchestration** using Apache Airflow DAGs.
- **Containerization** using Docker.

## 9. Project Limitations
This project is deliberately scoped for a local student portfolio. In a production environment:
- **Cloud Infrastructure:** We would use AWS/GCP instead of local Docker.
- **Managed Services:** We would use Confluent Cloud (Kafka), Databricks (Spark), and Snowflake (Warehouse).
- **Scale:** Real clusters would handle millions of records per second rather than local micro-batches.
- **Security:** We would implement SSL, IAM roles, and secrets management instead of hardcoded local passwords.

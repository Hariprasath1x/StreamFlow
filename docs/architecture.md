# StreamFlow Architecture

This document outlines the final event-driven streaming ELT architecture for the StreamFlow project.

```text
Python Order Generator
        ↓
Kafka Producer
        ↓
Kafka Broker
        ↓
orders topic
        ↓
PySpark Structured Streaming
        ↓
Validation + Transformation
        ↓
       ┌───────────────┐
       │               │
     VALID          INVALID
       │               │
       ↓               ↓
processed_orders  invalid_orders
       │
       ↓
PostgreSQL
       │
       ↓
Analytics / BI
```

## Batch Orchestration (Conceptual Demonstration)
```text
Airflow
   ↓
Batch orchestration demo
```

*(Note: Airflow orchestrates daily/nightly analytical batch jobs and does NOT control the continuous Spark Streaming pipeline.)*

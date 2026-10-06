# Requirements

## Functional Requirements
- **Data Generation:** Produce realistic, small-scale e-commerce orders mimicking a live system.
- **Message Streaming:** (Phase 2) Send generated orders to an Apache Kafka topic.
- **Data Processing:** (Phase 3) Use PySpark to consume Kafka messages, validate fields, and handle basic errors.
- **Storage:** (Phase 3) Load processed records into a PostgreSQL data warehouse.
- **Orchestration:** (Phase 4) Use Apache Airflow to schedule data pipelines.
- **Dashboard:** (Phase 5) Visualize key metrics (revenue, order counts) using Power BI.

## Non-Functional Requirements
- **Simplicity:** The project must be easy to run on a local machine for portfolio demonstrations.
- **Reproducibility:** Dependencies and environments must be managed via Conda/Pip and Docker.
- **Maintainability:** Code should be modular but not over-engineered.

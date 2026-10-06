# Data Engineering Interview Notes: StreamFlow

### Why Kafka?
Kafka was chosen to demonstrate event-driven architecture. In modern systems, business events (like orders) happen continuously. Kafka acts as a durable, highly scalable message broker that decouples the application generating orders from the analytics engine processing them, allowing the system to handle sudden traffic spikes (e.g., Black Friday sales).

### Why PySpark?
While a simple Python script could process small data, PySpark (Structured Streaming) was used to demonstrate knowledge of distributed computing. Spark can scale across hundreds of nodes to process terabytes of streaming data in parallel.

### Why PostgreSQL?
PostgreSQL was used to simulate a Data Warehouse. It's robust, supports complex analytical SQL queries, and connects easily to BI tools like Power BI.

### Why Airflow?
Apache Airflow was added to demonstrate batch workflow orchestration. While Kafka and Spark handle continuous real-time streams, a real business also has nightly jobs (e.g., historical data loads, backup routines, or ML model training). Airflow manages the scheduling, dependencies, and retries for these batch jobs.

### Why Docker?
Docker encapsulates infrastructure (Kafka, PostgreSQL) into isolated containers. This prevents dependency conflicts on the host machine, guarantees the environment is reproducible, and simplifies the local setup process to a single `docker compose up -d` command.

### Why not process everything directly in Python?
A single Python script reading a database and writing to a dashboard does not scale. If the script crashes, data is lost. If data volume grows 100x, the script will run out of memory. This architecture (Producer -> Kafka -> Spark -> Postgres) represents an enterprise pattern that scales horizontally.

### Batch vs Streaming
- **Batch Processing:** Processing data in large chunks at scheduled intervals (e.g., once a day). Slower insights, but cheaper and easier to manage.
- **Streaming Processing:** Processing data continuously as it arrives. Immediate insights (like live fraud detection), but requires more complex infrastructure like Kafka. This project demonstrates both (Spark for streaming, Airflow for batch).

### ETL vs ELT
- **ETL (Extract, Transform, Load):** Transforming data *before* loading it into the warehouse (done here by PySpark).
- **ELT (Extract, Load, Transform):** Loading raw data into a cloud data warehouse (like Snowflake) and transforming it *inside* the warehouse using SQL (dbt).

### What happens when invalid data arrives?
PySpark validates incoming events. If an order has a negative quantity or missing ID, it is filtered out of the main pipeline and written to an `invalid_orders` table (Dead Letter Queue pattern) for later debugging.

### What happens if the consumer is slower than the producer?
Kafka safely persists the incoming messages to disk. The consumer (PySpark) will process them as fast as it can without dropping data. The "lag" will increase, but the system won't crash.

### What happens if Kafka temporarily goes down?
The producer will fail to send messages. In a production setup, producers are configured with retries and dead-letter queues. Because this is a cluster, multiple Kafka brokers would normally prevent a total outage.

### Why does PySpark need Checkpointing?
Spark Structured Streaming uses a checkpoint directory (e.g., `data/checkpoints/`) to persist its streaming progress and state. Conceptually, it stores the Kafka offsets it has successfully processed. Without checkpointing, restarting the Spark job would cause it to lose track of where it left off, resulting in reprocessing previously consumed Kafka messages from the beginning (if `startingOffsets` is set to `earliest`) or missing messages (if set to `latest`). Checkpointing guarantees fault-tolerant, exactly-once (or at-least-once) processing semantics across restarts.


### Why use `foreachBatch`?
`foreachBatch` allows Structured Streaming to write micro-batches to systems that might not have a dedicated, perfectly optimized native streaming sink (or when applying complex custom logic per batch). Here, it easily writes processed PySpark DataFrames into PostgreSQL using standard JDBC appends.

### How would this system scale in production?
- **Current:** Local Python script -> Local Docker Kafka -> Local PySpark -> Local Docker Postgres.
- **Production:** Cloud API Gateway -> Managed Kafka (Confluent/MSK) -> Databricks/EMR cluster for PySpark -> Cloud Data Warehouse (Snowflake/Redshift/BigQuery). Airflow would be managed via MWAA or Cloud Composer.

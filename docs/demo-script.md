# StreamFlow Live Interview Demo Script

This script is designed to be executed during a 10-minute technical interview demonstration to prove the pipeline is fully functional end-to-end.

## PREREQUISITES
Ensure Docker is running and ports `9092` and `5432` are available.

---

## STEP 1: Start Infrastructure
Start the Kafka broker and PostgreSQL data warehouse.
```bash
docker compose up -d
```
*Wait 5-10 seconds for Kafka to initialize.*

## STEP 2: Clear Checkpoints (For Demo Reproducibility)
Remove the previous Spark streaming checkpoints so the demonstration starts completely fresh.
```bash
rm -rf data/checkpoints/*
```

## STEP 3: Start Spark Streaming
Launch the PySpark Structured Streaming job. It will immediately begin listening to the `orders` topic.
```bash
.venv/bin/python streaming/spark_streaming.py
```
*Keep this terminal open.*

## STEP 4: Run Kafka Producer
Open a **new terminal**. Start generating synthetic e-commerce orders and pushing them to Kafka.
```bash
.venv/bin/python streaming/kafka_producer.py
```
*Wait for it to send the 5 valid orders.*

## STEP 5: Show Spark Processing
Switch back to the Spark terminal.
Point out:
- The streaming query starts.
- It detects the new offsets.
- `Batch 0: Wrote 5 valid records to PostgreSQL.`

## STEP 6: Query Processed Orders
Open a **third terminal**. Query the local PostgreSQL database to prove the valid records successfully landed.
```bash
docker exec streamflow-postgres psql -U postgres -d streamflow -c "SELECT * FROM processed_orders;"
```

## STEP 7: Send an Invalid Order
Demonstrate the dead-letter queue (DLQ) pattern by intentionally generating malformed data.
Run the scratch script:
```bash
.venv/bin/python scratch_produce_invalid.py
```
*(Alternatively, explain that the script sends a negative quantity and invalid timestamp).*

## STEP 8: Verify Invalid Orders
Switch back to the Spark terminal. Note that it logged writing `INVALID records`.
Query PostgreSQL again:
```bash
docker exec streamflow-postgres psql -U postgres -d streamflow -c "SELECT * FROM invalid_orders;"
```
Point out the `error_reason` column.

## STEP 9: Explain Checkpointing
Point out the checkpoint directory:
```bash
ls -l data/checkpoints/
```
*Explain:* "If I kill the Spark job right now and restart it, it will read from these offsets and continue exactly where it left off without duplicating these records."

## STEP 10: Teardown
Cleanly shut down the infrastructure.
```bash
docker compose down
```

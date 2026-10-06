import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, hour, expr, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "streamflow")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secretpassword")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

PG_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
PG_PROPS = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def process_batch(batch_df, batch_id):
    """
    Function to process each micro-batch.
    Writes valid records to 'processed_orders' and invalid records to 'invalid_orders'.
    """
    if batch_df.isEmpty():
        return
        
    valid_df = batch_df.filter(
        (col("quantity") > 0) & 
        (col("unit_price") > 0) & 
        col("order_id").isNotNull() &
        col("order_timestamp").isNotNull()
    )
    
    # For invalid records, we just capture the order_id and a reason
    invalid_df = batch_df.filter(
        (col("quantity") <= 0) | 
        (col("unit_price") <= 0) | 
        col("order_id").isNull() |
        col("order_timestamp").isNull()
    ) \
        .withColumn("error_reason", lit("Data Quality Check Failed")) \
        .select("order_id", "error_reason")
                         
    # Write valid records to PostgreSQL
    if not valid_df.isEmpty():
        valid_df.write.jdbc(url=PG_URL, table="processed_orders", mode="append", properties=PG_PROPS)
        print(f"Batch {batch_id}: Wrote {valid_df.count()} valid records to PostgreSQL.")
        
    # Write invalid records to PostgreSQL
    if not invalid_df.isEmpty():
        # In a real system we'd capture the raw JSON, here we just insert what we parsed for simplicity
        invalid_df = invalid_df.withColumn("raw_data", lit("N/A"))
        invalid_df.write.jdbc(url=PG_URL, table="invalid_orders", mode="append", properties=PG_PROPS)
        print(f"Batch {batch_id}: Wrote {invalid_df.count()} INVALID records to PostgreSQL.")


def main():
    # Initialize Spark Session with Kafka and PostgreSQL packages
    print("Initializing PySpark Session...")
    spark = SparkSession.builder \
        .appName("StreamFlow-Kafka-To-Postgres") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
        
    # Suppress verbose logging
    spark.sparkContext.setLogLevel("WARN")

    # Define the schema of the incoming JSON events
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    print("Connecting to Kafka...")
    # Read from Kafka stream
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "orders") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON from Kafka value column
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    print("Applying transformations...")
    # Transformations: 
    # 1. Parse timestamp
    # 2. Extract date and hour
    # 3. Calculate total_amount
    transformed_df = parsed_df \
        .withColumn("order_timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("order_date", to_date(col("order_timestamp"))) \
        .withColumn("order_hour", hour(col("order_timestamp"))) \
        .withColumn("total_amount", col("quantity") * col("unit_price")) \
        .drop("timestamp")

    print("Starting Streaming Query...")
    # Write stream using foreachBatch
    query = transformed_df.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "data/checkpoints/") \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()

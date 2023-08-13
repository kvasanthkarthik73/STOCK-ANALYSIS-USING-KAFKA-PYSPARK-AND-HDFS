from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaBatchProcessing") \
    .getOrCreate()

kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "stonks"

# Read data from Kafka topic as a batch
kafka_data = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Convert binary value column to string
kafka_data = kafka_data.selectExpr("CAST(value AS STRING)")

# Show the extracted data
kafka_data.show(truncate=False)

# Write the extracted data to a text file
kafka_value.write \
    .mode("overwrite") \
    .parquet("hdfs:///user")

# Stop the SparkSession
spark.stop()


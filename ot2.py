from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ConvertParquetToText") \
    .getOrCreate()

# Define the HDFS directory containing Parquet files
hdfs_directory = "hdfs:///user/"

# Read Parquet files from the directory
parquet_data = spark.read.parquet(hdfs_directory)

# Write the Parquet data to text files
output_text_directory = "hdfs:///user/parquet_to_text_output"
parquet_data.write.mode("overwrite").text(output_text_directory)

# Read the converted text files
text_data = spark.read.text(output_text_directory)

# Show the contents of the text files (first few lines)
text_data.show(truncate=False)

# Stop the SparkSession
spark.stop()


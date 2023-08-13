from kafka import KafkaConsumer

# Define the Kafka bootstrap servers and topic
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "stonks"

# Create a Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    group_id="my_consumer_group"  # Choose a unique consumer group ID
)

# Consume and print messages from the Kafka topic
for message in consumer:
    print(message.value)

# Close the consumer
consumer.close()

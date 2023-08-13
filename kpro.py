import requests
from kafka import KafkaProducer
import json
# Define the Kafka bootstrap servers
kafka_bootstrap_servers = "localhost:9092"
kafka_topic= "stonks" 


# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
# Replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=3FAJUMCAAD47GQMF'
r = requests.get(url)
data = r.json()
# Send the data to the Kafka topic
producer.send(kafka_topic, value=data)

# Close the producer
producer.close()

print("Data sent to Kafka topic successfully!")

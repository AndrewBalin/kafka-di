from kafka_di import Producer
from kafka_di.config import Config

# Initialize the producer
config = Config(bootstrap_servers='localhost:9092', group_id='test-group')

producer = Producer(configs=config)

# Produce a message to a topic
producer.produce('test-topic', value=b'Hello, Kafka!')
producer.flush()

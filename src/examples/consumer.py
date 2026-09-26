from kafka_di import Kafka
from kafka_di.config import Config
from kafka_di.consumer import Consumer

config = Config(bootstrap_servers='localhost:9092', group_id='test-group')

app = Kafka(configs=config)

# Create a separate consumer
consumer = Consumer()


@consumer.subscribe('test-topic')
def handle(msg):
    print(msg)


# Register the consumer with the application
app.register_consumer(consumer)

app.run()

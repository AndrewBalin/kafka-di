from kafka_di import Kafka
from kafka_di.config import Config

# Initialize the application
config = Config(bootstrap_servers='localhost:9092', group_id='test-group')

app = Kafka(configs=config)


# Subscribe to a topic
@app.consumer.subscribe('test-topic')
def handle(msg):
    print(msg)


# Run the application
app.run()

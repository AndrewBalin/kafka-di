from kafka_di import Depends, Kafka, Producer
from kafka_di.config import Config

config = Config(bootstrap_servers='localhost:9092', group_id='test-group')

app = Kafka(configs=config)


# Dependency that provides a producer
def get_producer() -> Producer:
    return app.producer


# Subscribe to a topic with a dependency
@app.consumer.subscribe('test-topic')
def handle(msg, producer: Producer = Depends(get_producer)):
    print(msg)
    producer.produce('response-topic', value=b'ACK')


app.run()

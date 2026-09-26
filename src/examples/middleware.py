from kafka_di import Kafka
from kafka_di.config import Config
from kafka_di.middlewares import ConsumerMiddleware

config = Config(bootstrap_servers='localhost:9092', group_id='test-group')

app = Kafka(configs=config)


# Define middleware
class Middleware(ConsumerMiddleware):
    def handle(self, msg):
        print('Middleware executed')


# Register middleware
app.register_middleware(Middleware())


@app.consumer.subscribe('test-topic')
def handle(msg):
    print(msg)


app.run()

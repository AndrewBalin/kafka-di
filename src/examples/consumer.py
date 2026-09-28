from kafka_di import Kafka
from kafka_di.config import Config
from kafka_di.consumer import Consumer, ConsumerContext
from kafka_di.message.models import DecodedMessage

config = Config(bootstrap_servers='localhost:9092', group_id='audit-service')

app = Kafka(configs=config)
consumer = Consumer()


@consumer.subscribe('audit.raw')
def handle_raw_message(message: DecodedMessage, context: ConsumerContext):
    print(
        f'topic={message.topic} partition={message.partition} '
        f'offset={message.offset} key={message.key!r} value={message.value!r}'
    )

    if message.value is None:
        context.uncommit()


app.register_consumer(consumer)


if __name__ == '__main__':
    app.run()

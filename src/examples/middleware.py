from pydantic import BaseModel

from kafka_di import JsonCodec, Kafka
from kafka_di.config import Config
from kafka_di.message.models import DecodedMessage
from kafka_di.middlewares import ConsumerMiddleware


class UserRegistered(BaseModel):
    user_id: str


class LoggingMiddleware(ConsumerMiddleware):
    def handle(self, message: DecodedMessage):
        print(f'Received {message.topic} at offset {message.offset}')


config = Config(bootstrap_servers='localhost:9092', group_id='notifications-service')
app = Kafka(configs=config, codec=JsonCodec())
app.register_middleware(LoggingMiddleware())


@app.consumer.subscribe('users.registered', value_type=UserRegistered)
async def send_welcome_notification(event: UserRegistered):
    print(f'Sending a welcome notification to {event.user_id}')


if __name__ == '__main__':
    app.run()

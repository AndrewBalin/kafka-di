from pydantic import BaseModel

from kafka_di import JsonCodec, Producer
from kafka_di.config import Config


class OrderCreated(BaseModel):
    order_id: str
    customer_id: str


config = Config(bootstrap_servers='localhost:9092', group_id='examples-producer')


if __name__ == '__main__':
    producer = Producer(configs=config, codec=JsonCodec())
    producer.publish(
        'orders.created',
        OrderCreated(order_id='order-42', customer_id='customer-7'),
        key='order-42',
    )
    producer.flush()

from pydantic import BaseModel

from kafka_di import JsonCodec, Kafka
from kafka_di.config import Config


class OrderCreated(BaseModel):
    order_id: str
    customer_id: str


config = Config(bootstrap_servers='localhost:9092', group_id='orders-service')
app = Kafka(configs=config, codec=JsonCodec())


@app.consumer.subscribe('orders.created', value_type=OrderCreated)
async def handle_order(event: OrderCreated):
    print(f'Order {event.order_id} was created for customer {event.customer_id}')


if __name__ == '__main__':
    app.run()

from pydantic import BaseModel

from kafka_di import Depends, JsonCodec, Kafka, Producer
from kafka_di.config import Config


class OrderCreated(BaseModel):
    order_id: str


class InvoiceRequested(BaseModel):
    order_id: str


config = Config(bootstrap_servers='localhost:9092', group_id='billing-service')
app = Kafka(configs=config, codec=JsonCodec())


@app.consumer.subscribe('orders.created', value_type=OrderCreated)
def request_invoice(event: OrderCreated, producer: Producer = Depends()):
    producer.publish(
        'invoices.requested',
        InvoiceRequested(order_id=event.order_id),
        key=event.order_id,
    )


if __name__ == '__main__':
    app.run()

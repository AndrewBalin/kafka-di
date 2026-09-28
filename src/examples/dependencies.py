import asyncio

from pydantic import BaseModel

from kafka_di import ConsumerContext, Depends, JsonCodec, Kafka
from kafka_di.config import Config
from kafka_di.message.models import DecodedMessage


class OrderCreated(BaseModel):
    order_id: str


class DatabaseSession:
    async def save_order(self, event: OrderCreated):
        await asyncio.sleep(0)
        print(f'Saved order {event.order_id}')

    async def close(self):
        await asyncio.sleep(0)
        print('Closed database session')


class OrderService:
    def __init__(self, session: DatabaseSession):
        self.session = session

    async def handle(self, event: OrderCreated):
        await self.session.save_order(event)


async def get_database_session():
    session = DatabaseSession()
    try:
        yield session
    finally:
        await session.close()


async def get_order_service(
    session: DatabaseSession = Depends(get_database_session),
) -> OrderService:
    return OrderService(session)


config = Config(bootstrap_servers='localhost:9092', group_id='orders-worker')
app = Kafka(configs=config, codec=JsonCodec())


@app.consumer.subscribe('orders.created', value_type=OrderCreated)
async def handle_order(
    event: OrderCreated,
    service: OrderService = Depends(get_order_service),
    cached_service: OrderService = Depends(get_order_service),
    message: DecodedMessage = Depends(),
    context: ConsumerContext = Depends(),
):
    assert service is cached_service
    print(f'Processing offset {message.offset}')

    try:
        await service.handle(event)
    except Exception:
        context.uncommit()
        raise


if __name__ == '__main__':
    app.run()

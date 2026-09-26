import asyncio
import time

import docker
import pytest
from testcontainers.kafka import KafkaContainer

from kafka_di import Kafka
from kafka_di.di import Depends
from kafka_di.middlewares import ConsumerMiddleware


def _docker_is_available():
    try:
        docker.from_env().ping()
    except docker.errors.DockerException:
        return False
    return True


@pytest.fixture(scope='session')
def kafka_container():
    if not _docker_is_available():
        pytest.skip('Docker daemon is not available')

    with KafkaContainer('confluentinc/cp-kafka:7.4.0') as kafka:
        yield kafka


@pytest.fixture
def kafka_config(kafka_container):
    return {'bootstrap.servers': kafka_container.get_bootstrap_server(), 'auto.offset.reset': 'earliest'}


def test_produce_consume(kafka_config):
    config = kafka_config.copy()
    config['group.id'] = 'test-group-1'
    app = Kafka(configs=config)
    received_messages = []

    @app.consumer.subscribe('test-topic')
    def handle(msg):
        print(f'Received message: {msg.value.decode()}')
        received_messages.append(msg.value.decode())
        app.consumers[0].stop()

    import threading

    thread = threading.Thread(target=app.run)
    thread.daemon = True
    thread.start()

    time.sleep(2)

    print('Producing message...')
    app.producer.produce('test-topic', value='hello kafka')
    app.producer.flush()

    thread.join(timeout=15)
    assert 'hello kafka' in received_messages


def test_middleware(kafka_config):
    config = kafka_config.copy()
    config['group.id'] = 'test-group-middleware'
    app = Kafka(configs=config)
    middleware_called = []

    class TestMiddleware(ConsumerMiddleware):
        def handle(self, msg):
            print('Middleware called')
            middleware_called.append(True)

    app.register_middleware(TestMiddleware())

    @app.consumer.subscribe('middleware-topic')
    def handle(msg):
        print('Handler called')
        app.consumers[0].stop()

    import threading

    thread = threading.Thread(target=app.run)
    thread.daemon = True
    thread.start()

    time.sleep(2)
    print('Producing message for middleware test...')
    app.producer.produce('middleware-topic', value='test middleware')
    app.producer.flush()

    thread.join(timeout=15)
    assert len(middleware_called) > 0


def test_dependency_injection(kafka_config):
    config = kafka_config.copy()
    config['group.id'] = 'test-group-di'
    app = Kafka(configs=config)

    def get_db():
        return 'database_connection'

    received_data = {}

    @app.consumer.subscribe('di-topic')
    def handle(msg, db: str = Depends(get_db)):
        print(f'DI handler called with db={db}')
        received_data['db'] = db
        received_data['msg'] = msg.value.decode()
        app.consumers[0].stop()

    import threading

    thread = threading.Thread(target=app.run)
    thread.daemon = True
    thread.start()

    time.sleep(2)
    print('Producing message for DI test...')
    app.producer.produce('di-topic', value='test di')
    app.producer.flush()

    thread.join(timeout=15)
    assert received_data.get('db') == 'database_connection'
    assert received_data.get('msg') == 'test di'


def test_async_handler_uses_aio_consumer(kafka_config):
    config = kafka_config.copy()
    config['group.id'] = 'test-group-async'
    app = Kafka(configs=config)
    received_messages = []

    @app.consumer.subscribe('async-topic')
    async def handle(msg):
        await asyncio.sleep(0.1)
        received_messages.append(msg.value.decode())
        app.consumers[0].stop()

    import threading

    thread = threading.Thread(target=app.run)
    thread.daemon = True
    thread.start()

    time.sleep(2)
    app.producer.produce('async-topic', value='hello async')
    app.producer.flush()

    thread.join(timeout=15)
    assert received_messages == ['hello async']


def test_context_uncommit_leaves_message_unprocessed(kafka_config):
    first_config = kafka_config.copy()
    first_config['group.id'] = 'test-group-uncommit'

    first_app = Kafka(configs=first_config)
    first_run_messages = []

    @first_app.consumer.subscribe('uncommit-topic')
    def handle_first(msg, context):
        first_run_messages.append(msg.value.decode())
        context.uncommit()
        first_app.consumers[0].stop()

    import threading

    first_thread = threading.Thread(target=first_app.run)
    first_thread.daemon = True
    first_thread.start()

    time.sleep(2)
    first_app.producer.produce('uncommit-topic', value='retry me')
    first_app.producer.flush()

    first_thread.join(timeout=15)
    assert first_run_messages == ['retry me']

    second_config = kafka_config.copy()
    second_config['group.id'] = 'test-group-uncommit'
    second_app = Kafka(configs=second_config)
    second_run_messages = []

    @second_app.consumer.subscribe('uncommit-topic')
    def handle_second(msg):
        second_run_messages.append(msg.value.decode())
        second_app.consumers[0].stop()

    second_thread = threading.Thread(target=second_app.run)
    second_thread.daemon = True
    second_thread.start()

    time.sleep(2)
    second_thread.join(timeout=15)

    assert second_run_messages == ['retry me']

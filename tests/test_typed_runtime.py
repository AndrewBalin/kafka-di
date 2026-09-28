from dataclasses import dataclass

import pytest

from kafka_di.codecs import JsonCodec
from kafka_di.consumer import Consumer, ConsumerContext
from kafka_di.consumer.handler import Handler
from kafka_di.di import Depends
from kafka_di.message.models import DecodedMessage
from kafka_di.producer import Producer


@dataclass
class Event:
    name: str


def decoded_message(value):
    return DecodedMessage(topic='events', key=None, value=value)


@pytest.mark.asyncio
async def test_typed_handler_resolves_nested_dependencies_once_and_cleans_up():
    calls = []

    def resource():
        calls.append('open')
        try:
            yield object()
        finally:
            calls.append('close')

    async def async_resource():
        calls.append('async open')
        try:
            yield object()
        finally:
            calls.append('async close')

    async def service(value=Depends(resource)):
        return value

    received = {}

    async def handle(
        event: Event,
        first=Depends(service),
        second=Depends(service),
        async_value=Depends(async_resource),
    ):
        received['event'] = event
        received['same'] = first is second
        received['async'] = async_value is not None

    event = Event(name='created')
    await Handler(handle, value_type=Event)(decoded_message(event), ConsumerContext())

    assert received == {'event': event, 'same': True, 'async': True}
    assert calls == ['open', 'async open', 'async close', 'close']


@pytest.mark.asyncio
async def test_dependency_use_cache_false_calls_provider_each_time():
    calls = 0

    def value():
        nonlocal calls
        calls += 1
        return calls

    received = []

    def handle(message, first=Depends(value, use_cache=False), second=Depends(value, use_cache=False)):
        received.extend((message, first, second))

    message = decoded_message(b'value')
    await Handler(handle)(message, ConsumerContext())

    assert received == [message, 1, 2]


@pytest.mark.asyncio
async def test_consumer_decodes_subscription_payload_and_honours_uncommit():
    consumer = Consumer()
    received = []

    @consumer.subscribe('events', value_type=Event, codec=JsonCodec())
    async def handle(event: Event, context: ConsumerContext):
        received.append(event)
        context.uncommit()

    class KafkaMessage:
        def topic(self):
            return 'events'

        def key(self):
            return None

        def value(self):
            return b'{"name":"created"}'

        def headers(self):
            return [('content-type', b'application/json')]

        def partition(self):
            return 0

        def offset(self):
            return 1

        def timestamp(self):
            return (0, 123)

    class KafkaConsumer:
        def __init__(self):
            self.commits = []

        def commit(self, **kwargs):
            self.commits.append(kwargs)

    kafka_consumer = KafkaConsumer()
    consumer._consumer = kafka_consumer

    await consumer._process_message(KafkaMessage())

    assert received == [Event(name='created')]
    assert kafka_consumer.commits == []


def test_json_codec_supports_dataclasses():
    codec = JsonCodec()

    encoded = codec.encode(Event(name='created'), target_type=Event)

    assert encoded == b'{"name":"created"}'
    assert codec.decode(encoded, target_type=Event) == Event(name='created')


def test_publish_encodes_value_and_key_with_codec():
    producer = Producer.__new__(Producer)
    producer._codec = JsonCodec()
    calls = []
    producer.produce = lambda topic, **kwargs: calls.append((topic, kwargs))

    producer.publish('events', Event(name='created'), key='event-1')

    assert calls == [
        (
            'events',
            {
                'value': b'{"name":"created"}',
                'key': b'"event-1"',
                'headers': None,
            },
        )
    ]

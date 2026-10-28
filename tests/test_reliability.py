from types import SimpleNamespace

import pytest

from kafka_di import Consumer, Kafka, RetryPolicy
from kafka_di.consumer.reliability import DeadLetterPublisher


@pytest.mark.asyncio
async def test_transient_failure_retries_same_message_before_advancing():
    consumer = Consumer(retry_policy=RetryPolicy(initial_delay=0))
    attempts, committed = [], []
    message = object()

    async def handle(record):
        attempts.append(record)
        if len(attempts) < 3:
            raise OSError('temporary database outage')
        await consumer._commit(record)

    consumer._process_message = handle
    consumer._consumer = SimpleNamespace(commit=lambda **kwargs: committed.append(kwargs))
    await consumer._process_with_retries(message)
    assert attempts == [message] * 3
    assert committed == [{'message': message, 'asynchronous': False}]
    assert consumer.metrics['retries'] == 2


@pytest.mark.asyncio
async def test_poison_is_quarantined_before_commit_and_dlq_failure_stops_progress():
    order = []

    def publish(*args):
        order.append('dlq')
        raise OSError('broker unavailable')

    consumer = Consumer(
        retry_policy=RetryPolicy(max_attempts=1), dead_letter_publisher=SimpleNamespace(publish=publish)
    )

    async def invalid(record):
        raise ValueError('invalid event')

    consumer._process_message = invalid
    consumer._consumer = SimpleNamespace(commit=lambda **kwargs: order.append('commit'))
    with pytest.raises(OSError):
        await consumer._process_with_retries(object())
    assert order == ['dlq']
    consumer._dead_letter_publisher.publish = lambda *args: order.append('acknowledged')
    await consumer._process_with_retries(object())
    assert order[-2:] == ['acknowledged', 'commit']


def test_quarantine_preserves_event_id_and_excludes_sensitive_exception_text():
    published = []
    publisher = DeadLetterPublisher.__new__(DeadLetterPublisher)
    publisher.group = 'test'

    def produce(**kwargs):
        published.append(kwargs)
        kwargs['on_delivery'](None, None)

    publisher.producer = SimpleNamespace(produce=produce, flush=lambda timeout: 0)
    message = SimpleNamespace(
        topic=lambda: 'events',
        partition=lambda: 2,
        offset=lambda: 7,
        key=lambda: b'person',
        value=lambda: b'encrypted-payload',
        headers=lambda: [('event_id', b'stable-event')],
    )
    publisher.publish(message, ValueError('OTP secret must not be logged'), 4)
    headers = dict(published[0]['headers'])
    assert headers['event_id'] == b'stable-event'
    assert headers['x-kafka-di-original-offset'] == b'7'
    assert published[0]['value'] == b'encrypted-payload'
    assert 'OTP secret' not in str(headers)


def test_background_consumer_failure_reaches_supervisor_and_stops_siblings():
    from threading import Event

    started, stopped = Event(), Event()

    def waiting():
        started.set()
        assert stopped.wait(timeout=5)

    def failing():
        assert started.wait(timeout=5)
        raise RuntimeError('database and quarantine are unavailable')

    app = Kafka()
    app.consumers = [
        SimpleNamespace(run=waiting, stop=stopped.set),
        SimpleNamespace(run=failing, stop=lambda: None),
    ]
    with pytest.raises(RuntimeError, match='quarantine'):
        app.run()
    assert stopped.is_set()

"""Explicit bounded DLQ replay. Never prints event bodies or credentials."""

import argparse
import json
import os

from confluent_kafka import Consumer, Producer


def replay(configs, topic, target, limit):
    if topic == target:
        raise ValueError('source and target must differ')
    consumer = Consumer(
        configs
        | {
            'group.id': f'replay:{topic}',
            'enable.auto.commit': False,
            'enable.auto.offset.store': False,
            'auto.offset.reset': 'earliest',
        }
    )
    producer = Producer(
        {
            key: value
            for key, value in configs.items()
            if key == 'bootstrap.servers' or key.startswith(('security.', 'sasl.', 'ssl.'))
        }
        | {'enable.idempotence': True}
    )
    consumer.subscribe([topic])
    count = 0
    try:
        while count < limit:
            message = consumer.poll(10)
            if message is None:
                break
            if message.error():
                raise RuntimeError('cannot read dead letter event')
            headers = message.headers() or []
            original = dict(headers).get('x-kafka-di-original-topic')
            if original != target.encode():
                raise ValueError('replay target differs from original topic')
            failures, acknowledgments = [], []

            def acknowledged(error, _message):
                acknowledgments.append(True)
                if error is not None:
                    failures.append(error)

            producer.produce(
                target,
                key=message.key(),
                value=message.value(),
                headers=[(key, value) for key, value in headers if not key.startswith('x-kafka-di-')],
                on_delivery=acknowledged,
            )
            if producer.flush(10) or failures or not acknowledgments:
                raise RuntimeError('replay delivery was not acknowledged')
            consumer.commit(message=message, asynchronous=False)
            count += 1
    finally:
        consumer.close()
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dlq_topic')
    parser.add_argument('original_topic')
    parser.add_argument('--limit', type=int, default=100)
    args = parser.parse_args()
    if not 1 <= args.limit <= 10000:
        parser.error('limit must be 1..10000')
    count = replay(json.loads(os.environ['KAFKA_REPLAY_CONFIG']), args.dlq_topic, args.original_topic, args.limit)
    print(f'Replayed {count} events')

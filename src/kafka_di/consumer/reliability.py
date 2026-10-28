"""Bounded retry and acknowledged quarantine, preserving the original wire event."""

from dataclasses import dataclass

from confluent_kafka import Producer


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 4
    initial_delay: float = 1
    max_delay: float = 30

    def __post_init__(self):
        if not 1 <= self.max_attempts <= 20 or not 0 <= self.initial_delay <= self.max_delay <= 60:
            raise ValueError('invalid retry policy')


class DeadLetterPublisher:
    def __init__(self, configs):
        allowed = ('bootstrap.servers', 'security.protocol', 'sasl.', 'ssl.')
        self.producer = Producer(
            {key: value for key, value in configs.items() if any(key == p or key.startswith(p) for p in allowed)}
            | {'enable.idempotence': True, 'acks': 'all'}
        )
        self.group = configs['group.id']

    def publish(self, message, error, attempts):
        headers = [(key, value) for key, value in (message.headers() or []) if not key.startswith('x-kafka-di-')]
        headers.extend(
            (key, str(value).encode())
            for key, value in {
                'x-kafka-di-original-topic': message.topic(),
                'x-kafka-di-original-partition': message.partition(),
                'x-kafka-di-original-offset': message.offset(),
                'x-kafka-di-consumer-group': self.group,
                'x-kafka-di-error-type': type(error).__name__,
                'x-kafka-di-attempts': attempts,
            }.items()
        )
        failures, delivered = [], []

        def acknowledged(error, _message):
            delivered.append(True)
            if error is not None:
                failures.append(error)

        self.producer.produce(
            topic=f'{message.topic()}.{self.group}.dlq',
            key=message.key(),
            value=message.value(),
            headers=headers,
            on_delivery=acknowledged,
        )
        if self.producer.flush(10) or failures or not delivered:
            raise RuntimeError('dead letter delivery was not acknowledged')

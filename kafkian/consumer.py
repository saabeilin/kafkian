import socket
import typing
from typing import Callable

import structlog
from confluent_kafka.cimpl import Consumer as ConfluentConsumer, KafkaError
from confluent_kafka.cimpl import KafkaException

from kafkian.serde.deserialization import Deserializer

logger = structlog.get_logger(__name__)


class Consumer:

    DEFAULT_CONFIG = {
        'api.version.request': True,
        'client.id': socket.gethostname(),
        'default.topic.config': {
            'auto.offset.reset': 'latest'
        },
        'enable.auto.commit': False,
        'fetch.error.backoff.ms': 0,
        'fetch.wait.max.ms': 10,
        'log.connection.close': False,
        'log.thread.name': False,
        'session.timeout.ms': 6000,
        'statistics.interval.ms': 15000
    }

    def __init__(
        self, config: typing.Dict, topics: typing.Iterable,
            value_deserializer=Deserializer(), key_deserializer=Deserializer(),
            error_handler: Callable = None
    ) -> None:
        self._subscribed = False
        self.topics = list(topics)
        self.non_blocking = False   # TODO
        self.timeout = 0.1          # TODO
        self.key_deserializer = key_deserializer
        self.value_deserializer = value_deserializer
        logger.info("Initializing consumer", config=config)
        self._consumer_impl = self._init_consumer_impl(config)
        self._generator = self._message_generator()

    @staticmethod
    def _init_consumer_impl(config):
        return ConfluentConsumer(config)

    def _subscribe(self):
        if self._subscribed:
            return
        self._consumer_impl.subscribe(self.topics)
        self._subscribed = True

    def __iter__(self):
        return self

    def __next__(self):
        self._subscribe()
        try:
            return next(self._generator)
        except KafkaException:
            raise StopIteration

    def __enter__(self):
        self._consumer_impl.subscribe(self.topics)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        # the only reason a consumer exits is when an
        # exception is raised.
        #
        # close down the consumer cleanly accordingly:
        #  - stops consuming
        #  - commit offsets (only on auto commit)
        #  - leave consumer group
        logger.info("Closing consumer")
        self.consumer.close()

    def _message_generator(self):
        while True:
            message = self._consumer_impl.poll(timeout=self.timeout)
            if message is None:
                if self.non_blocking:
                    yield None
                continue
            if message.error() == KafkaError._PARTITION_EOF:
                continue
            yield self._deserialize(message)

    def _deserialize(self, message):
        message.set_key(self.key_deserializer.deserialize(message.key()))
        message.set_value(self.key_deserializer.deserialize(message.value()))
        return message

    @property
    def is_auto_commit(self):
        return self.config.get('enable.auto.commit', True)

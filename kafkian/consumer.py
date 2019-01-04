import atexit
import socket
import typing
from typing import Callable

import structlog
from confluent_kafka.cimpl import Consumer as ConfluentConsumer, KafkaError

from kafkian.exceptions import KafkianException
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
            error_callbacks: typing.List[Callable] = None
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
        self.error_callbacks = error_callbacks
        atexit.register(self._close)

    @staticmethod
    def _init_consumer_impl(config):
        return ConfluentConsumer(config)

    def _subscribe(self):
        if self._subscribed:
            return
        self._consumer_impl.subscribe(self.topics)
        self._subscribed = True

    def __iter__(self):
        self._subscribe()
        return self

    def __next__(self):
        self._subscribe()
        try:
            return next(self._generator)
        except:
            self._close()
            raise

    def __enter__(self):
        self._consumer_impl.subscribe(self.topics)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self._close()

    def _close(self):
        """
        Close down the consumer cleanly accordingly:
         - stops consuming
         - commit offsets (only on auto commit)
         - leave consumer group
        """
        logger.info("Closing consumer")
        try:
            self._consumer_impl.close()
        except RuntimeError:
            # Consumer is probably already closed
            pass

    def _poll(self):
        return self._consumer_impl.poll(timeout=self.timeout)

    def _message_generator(self):
        while True:
            message = self._poll()
            if message is None:
                if self.non_blocking:
                    yield None
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                self._on_poll_error(message)
                raise KafkianException(message.error())
            yield self._deserialize(message)

    def _deserialize(self, message):
        message.set_key(self.key_deserializer.deserialize(message.key()))
        value = message.value()
        # If value is None, it's a tombstone, just pass it through
        if value is not None:
            value = self.value_deserializer.deserialize(value)
        message.set_value(value)
        return message

    def commit(self, sync=False):
        """
        Commits current consumer offsets.
        :param sync: do a synchronous commit (false by default)
        """
        self._consumer_impl.commit(asynchronous=not sync)

    def _on_poll_error(self, message):
        if self.error_callbacks:
            for cb in self.error_callbacks:
                cb(message)

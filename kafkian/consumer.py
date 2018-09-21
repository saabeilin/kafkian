import socket
from typing import Callable

import structlog
from confluent_kafka.cimpl import Consumer as ConfluentConsumer
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
        self, config,
            value_serializer=Deserializer(), key_serializer=Deserializer(),
            error_handler: Callable = None
    ) -> None:
        stop_on_eof = config.pop('stop_on_eof', False)
        poll_timeout = config.pop('poll_timeout', 0.1)
        self.non_blocking = config.pop('non_blocking', False)
        self.topics = self._get_topics(self.config)

        logger.info("Initializing consumer", config=self.config)
        self._consumer_impl = ConfluentConsumer(self.config)
        self._generator = self._message_generator()

    def __getattr__(self, name):
        return getattr(self.consumer, name)

    def __iter__(self):
        return self

    def __next__(self):
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
            yield Message(message)

    def _get_topics(self, config):
        topics = config.pop('topics', None)
        assert topics is not None, "You must subscribe to at least one topic"

        if not isinstance(topics, list):
            topics = [topics]

        return topics

    @property
    def is_auto_commit(self):
        return self.config.get('enable.auto.commit', True)

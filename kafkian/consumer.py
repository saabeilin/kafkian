import atexit
import logging
import socket
import typing

from confluent_kafka.cimpl import Consumer as ConfluentConsumer, KafkaError

from kafkian.exceptions import KafkianException
from kafkian.message import Message
from kafkian.serde.deserialization import Deserializer

logger = logging.getLogger(__name__)


class Consumer:
    """
    Kafka consumer with configurable key/value deserializers.

    Can be used both as a context manager or generator.

    Does not subclass directly from Confluent's Consumer,
    since it's a cimpl and therefore not mockable.
    """

    # Default configuration. For more details, description and defaults, see
    # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    DEFAULT_CONFIG = {
        'api.version.request': True,
        'log.connection.close': True,
        'log.thread.name': False,
        'client.id': socket.gethostname(),
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False,
        'queued.min.messages': 1000,
        'fetch.error.backoff.ms': 0,
        'fetch.wait.max.ms': 10,
        'session.timeout.ms': 6000,
        'statistics.interval.ms': 15000
    }

    def __init__(
            self,
            config: typing.Dict,
            topics: typing.Iterable,
            value_deserializer=Deserializer(),
            key_deserializer=Deserializer(),
            error_callback: typing.Optional[typing.Callable] = None,
            commit_success_callback: typing.Optional[typing.Callable] = None,
            commit_error_callback: typing.Optional[typing.Callable] = None,
            metrics=None
    ) -> None:

        self._subscribed = False
        self.topics = list(topics)
        self.non_blocking = False  # TODO
        self.timeout = 0.1  # TODO
        self.key_deserializer = key_deserializer
        self.value_deserializer = value_deserializer

        self.error_callback = error_callback
        self.commit_success_callback = commit_success_callback
        self.commit_error_callback = commit_error_callback

        self.metrics = metrics

        config = {**self.DEFAULT_CONFIG, **config}
        config['on_commit'] = self._on_commit
        config['error_cb'] = self._on_error
        config['throttle_cb'] = self._on_throttle
        config['stats_cb'] = self._on_stats

        logger.info("Initializing consumer", extra=dict(config=config))
        atexit.register(self._close)
        self._consumer_impl = self._init_consumer_impl(config)
        self._generator = self._message_generator()

    @staticmethod
    def _init_consumer_impl(config):
        return ConfluentConsumer(config, logger=logging.getLogger('librdkafka.consumer'))

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
                raise KafkianException(message.error())
            yield Message(message, self.key_deserializer, self.value_deserializer)

    def commit(self, sync=False):
        """
        Commits current consumer offsets.

        :param sync: do a synchronous commit (false by default)
        """
        return self._consumer_impl.commit(asynchronous=not sync)

    def _on_commit(self, err, topics_partitions):
        if err:
            logger.warning(
                "Offset commit failed",
                extra=dict(error_message=str(err))
            )
            if self.commit_error_callback:
                self.commit_error_callback(topics_partitions, err)
        else:
            logger.debug(
                "Offset commit succeeded",
                extra=dict(topics_partitions=topics_partitions)
            )
            if self.commit_success_callback:
                self.commit_success_callback(topics_partitions)

    def _on_error(self, error: KafkaError):
        logger.error(error.str(), extra=dict(error_code=error.code(), error_name=error.name()))
        if self.error_callback:
            self.error_callback(error)

    def _on_throttle(self, event):
        logger.warning("Throttle", extra=dict(throttle_event=event))

    def _on_stats(self, stats):
        if self.metrics:
            self.metrics.send(stats)

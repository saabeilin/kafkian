import atexit
import socket

import structlog
from confluent_kafka.cimpl import Producer as ConfluentProducer

from kafkian.serde.serialization import Serializer

logger = structlog.get_logger(__name__)


class Producer:
    """
    Kafka producer with configurable key/value serializers.

    Does not subclass directly from Confluent's Producer,
    since it's a cimpl and therefore not mockable.
    """

    DEFAULT_CONFIG = {
        'acks': 'all',
        'api.version.request': True,
        'client.id': socket.gethostname(),
        'log.connection.close': False,
        'max.in.flight': 1,
        'queue.buffering.max.ms': 100,
        'statistics.interval.ms': 15000,
    }

    def __init__(self, config,
                 value_serializer=Serializer(), key_serializer=Serializer(),
                 get_callback=None):  # yapf: disable
        config = {**self.DEFAULT_CONFIG, **config}
        self.value_serializer = value_serializer
        self.key_serializer = key_serializer

        logger.info("Initializing producer", config=config)
        atexit.register(self._close)

        self._producer_impl = self._init_producer_impl(config)

    @staticmethod
    def _init_producer_impl(config):
        return ConfluentProducer(config)

    def _close(self):
        self.flush()

    def flush(self, timeout=None):
        logger.info("Flushing producer")
        if timeout:
            self._producer_impl.flush(timeout)
        else:
            self._producer_impl.flush()

    def poll(self, timeout=None):
        timeout = timeout or 1
        return self._producer_impl.poll(timeout)

    def produce(self, topic, key, value, sync=False):
        key = self.key_serializer.serialize(key, topic, is_key=True)
        # If value is None, it's a "tombstone" and shall be passed through
        if value is not None:
            value = self.value_serializer.serialize(value, topic)
        self._produce(topic, key, value)
        if sync:
            self.flush()

    def _produce(self, topic, key, value, **kwargs):
        self._producer_impl.produce(topic=topic, value=value, key=key, **kwargs)

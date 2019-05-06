import atexit
import logging
import socket
import typing

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
        'enable.idempotence': True,
        'queue.buffering.max.ms': 100,
        'statistics.interval.ms': 15000,
    }

    def __init__(
            self,
            config: typing.Dict,
            value_serializer=Serializer(),
            key_serializer=Serializer(),
            error_callback: typing.Optional[typing.Callable] = None,
            delivery_success_callback: typing.Optional[typing.Callable] = None,
            delivery_error_callback: typing.Optional[typing.Callable] = None,
            metrics=None
    ) -> None:

        self.value_serializer = value_serializer
        self.key_serializer = key_serializer

        self.error_callback = error_callback
        self.delivery_success_callback = delivery_success_callback
        self.delivery_error_callback = delivery_error_callback

        self.metrics = metrics

        config = {**self.DEFAULT_CONFIG, **config}
        config['on_delivery'] = self._on_delivery
        config['error_cb'] = self._on_error
        config['throttle_cb'] = self._on_throttle
        config['stats_cb'] = self._on_stats

        logger.info("Initializing producer", config=config)
        atexit.register(self._close)
        self._producer_impl = self._init_producer_impl(config)

    @staticmethod
    def _init_producer_impl(config):
        return ConfluentProducer(config, logger=logging.getLogger('librdkafka.producer'))

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

    def _on_delivery(self, err, msg):
        if err:
            logger.warning(
                "Producer send failed",
                error_message=str(err),
                topic=msg.topic(),
                key=msg.key(),
                partition=msg.partition()
            )
            if self.delivery_error_callback:
                self.delivery_error_callback(msg, err)
        else:
            logger.debug(
                "Producer send succeeded",
                topic=msg.topic(),
                key=msg.key(),
                partition=msg.partition()
            )
            if self.delivery_success_callback:
                self.delivery_success_callback(msg)

    def _on_error(self, error):
        logger.error("Error", error=error)
        if self.error_callback:
            self.error_callback(error)

    def _on_throttle(self, event):
        logger.warning("Throttle", tevent=event)

    def _on_stats(self, stats):
        if self.metrics:
            self.metrics.send(stats)

import atexit
import logging
import socket
import typing

from confluent_kafka.cimpl import KafkaError, Producer as ConfluentProducer

from kafkian.serde.serialization import Serializer

logger = logging.getLogger(__name__)


class Producer:
    """
    Kafka producer with configurable key/value serializers.

    Does not subclass directly from Confluent's Producer,
    since it's a cimpl and therefore not mockable.
    """

    # Default configuration. For more details, description and defaults, see
    # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    DEFAULT_CONFIG = {
        'api.version.request': True,
        'client.id': socket.gethostname(),
        'log.connection.close': True,
        'log.thread.name': False,
        'acks': 'all',
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

        logger.info("Initializing producer", extra=dict(config=config))
        atexit.register(self._close)
        self._producer_impl = self._init_producer_impl(config)

    @staticmethod
    def _init_producer_impl(config):
        return ConfluentProducer(config, logger=logging.getLogger('librdkafka.producer'))

    def _close(self):
        self.flush()

    def flush(self, timeout=None):
        """
        Waits for all messages in the producer queue to be delivered and calls registered callbacks

        :param timeout:
        :return:
        """
        logger.info("Flushing producer")
        timeout = timeout or 1
        self._producer_impl.flush(timeout)

    def poll(self, timeout=None):
        """
        Polls the underlying producer for events and calls registered callbacks

        :param timeout:
        :return:
        """
        timeout = timeout or 1
        return self._producer_impl.poll(timeout)

    def produce(self, topic, key, value, sync=False):
        """
        Produces (`key`, `value`) to the specified `topic`.
        If `sync` is True, waits until the message is delivered/acked.

        Note that it does _not_ poll when sync if False.

        :param topic:
        :param key:
        :param value:
        :param sync:
        :return:
        """
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
                extra=dict(
                    error_message=str(err),
                    topic=msg.topic(),
                    key=msg.key(),
                    partition=msg.partition()
                )
            )
            if self.delivery_error_callback:
                self.delivery_error_callback(msg, err)
        else:
            logger.debug(
                "Producer send succeeded",
                extra=dict(
                    topic=msg.topic(),
                    key=msg.key(),
                    partition=msg.partition()
                )
            )
            if self.delivery_success_callback:
                self.delivery_success_callback(msg)

    def _on_error(self, error: KafkaError):
        logger.error(error.str(), extra=dict(error_code=error.code(), error_name=error.name()))
        if self.error_callback:
            self.error_callback(error)

    def _on_throttle(self, event):
        logger.warning("Throttle", extra=dict(throttle_event=event))

    def _on_stats(self, stats):
        if self.metrics:
            self.metrics.send(stats)

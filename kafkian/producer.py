import atexit
import logging
import socket
import typing

from confluent_kafka.cimpl import KafkaError
from confluent_kafka.cimpl import Producer as ConfluentProducer

from kafkian.serde.serialization import Serializer

logger = logging.getLogger(__name__)

DEFAULT_SERIALIZER = Serializer()


class Producer:
    """Kafka producer with configurable key/value serializers.

    Does not subclass directly from Confluent's Producer,
    since it's a cimpl and therefore not mockable.
    """

    # Default configuration. For more details, description and defaults, see
    # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    DEFAULT_CONFIG = {
        "api.version.request": True,
        "client.id": socket.gethostname(),
        "log.connection.close": True,
        "log.thread.name": False,
        "acks": "all",
        "max.in.flight": 1,
        "enable.idempotence": True,
        "queue.buffering.max.ms": 100,
        "statistics.interval.ms": 15000,
    }

    def __init__(
        self,
        config: dict,
        value_serializer=DEFAULT_SERIALIZER,
        key_serializer=DEFAULT_SERIALIZER,
        error_callback: typing.Callable | None = None,
        delivery_success_callback: typing.Callable | None = None,
        delivery_error_callback: typing.Callable | None = None,
        metrics=None,
    ) -> None:
        self.value_serializer = value_serializer
        self.key_serializer = key_serializer

        self.error_callback = error_callback
        self.delivery_success_callback = delivery_success_callback
        self.delivery_error_callback = delivery_error_callback

        self.metrics = metrics

        config = {**self.DEFAULT_CONFIG, **config}
        config["on_delivery"] = self._on_delivery
        config["error_cb"] = self._on_error
        config["throttle_cb"] = self._on_throttle
        config["stats_cb"] = self._on_stats

        logger.info("Initializing producer", extra={"config": config})
        atexit.register(self._close)
        self._producer_impl = self._init_producer_impl(config)

    @staticmethod
    def _init_producer_impl(config: dict[str, typing.Any]) -> ConfluentProducer:
        return ConfluentProducer(
            config, logger=logging.getLogger("librdkafka.producer")
        )

    def _close(self) -> None:
        self.flush()

    def flush(self, timeout: float | None = None) -> None:
        """Waits for all messages in the producer queue to be delivered
        and calls registered callbacks

        :param timeout:
        :return:
        """
        logger.info("Flushing producer")
        timeout = timeout or 1
        self._producer_impl.flush(timeout)

    def poll(self, timeout: float | None = None) -> int:
        """Polls the underlying producer for events and calls registered callbacks

        :param timeout:
        :return:
        """
        timeout = timeout or 1
        return self._producer_impl.poll(timeout)

    def produce(
        self,
        topic: str,
        key,
        value,
        headers: dict[str, str] | None = None,
        sync: bool = False,
    ) -> None:
        """Produces (`key`, `value`) to the specified `topic`.
        If `sync` is True, waits until the message is delivered/acked.

        Note that it does _not_ poll when sync if False.

        :param topic:
        :param key:
        :param value:
        :param sync:
        :param headers:
        :return:
        """
        key = self.key_serializer.serialize(key, topic, is_key=True)
        # If value is None, it's a "tombstone" and shall be passed through
        if value is not None:
            value = self.value_serializer.serialize(value, topic)
        headers = headers or {}
        self._produce(topic, key, value, headers)
        if sync:
            self.flush()

    def _produce(
        self, topic: str, key, value, headers: dict[str, str], **kwargs
    ) -> None:
        self._producer_impl.produce(
            topic=topic,
            value=value,
            key=key,
            headers=headers,
            **kwargs,
        )

    def _on_delivery(self, err, msg) -> None:
        if err:
            logger.warning(
                "Producer send failed",
                extra={
                    "error_message": str(err),
                    "topic": msg.topic(),
                    "key": msg.key(),
                    "partition": msg.partition(),
                },
            )
            if self.delivery_error_callback:
                self.delivery_error_callback(msg, err)
        else:
            logger.debug(
                "Producer send succeeded",
                extra={
                    "topic": msg.topic(),
                    "key": msg.key(),
                    "partition": msg.partition(),
                },
            )
            if self.delivery_success_callback:
                self.delivery_success_callback(msg)

    def _on_error(self, error: KafkaError) -> None:
        logger.error(
            error.str(), extra={"error_code": error.code(), "error_name": error.name()}
        )
        if self.error_callback:
            self.error_callback(error)

    def _on_throttle(self, event) -> None:
        logger.warning("Throttle", extra={"throttle_event": event})

    def _on_stats(self, stats):
        if self.metrics:
            self.metrics.send(stats)

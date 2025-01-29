import atexit
import logging
import socket
import struct
import typing
from types import TracebackType

from confluent_kafka.cimpl import Consumer as ConfluentConsumer
from confluent_kafka.cimpl import KafkaError, TopicPartition
from confluent_kafka.cimpl import Message as ConfluentMessage
from confluent_kafka.schema_registry import _MAGIC_BYTE, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, _ContextStringIO
from confluent_kafka.serialization import MessageField, SerializationContext

from kafkian.confluent_kafka_shims.json_schema import JSONDeserializer
from kafkian.exceptions import KafkianException
from kafkian.message import Message
from kafkian.metrics import KafkaMetrics

logger = logging.getLogger(__name__)


class Consumer:
    """Kafka consumer with configurable key/value deserializers.

    Can be used both as a context manager or generator.

    Does not subclass directly from Confluent's Consumer,
    since it's a cimpl and therefore not mockable.
    """

    # Default configuration. For more details, description and defaults, see
    # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    DEFAULT_CONFIG = {
        "api.version.request": True,
        "log.connection.close": True,
        "log.thread.name": False,
        "client.id": socket.gethostname(),
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "queued.min.messages": 1000,
        "fetch.error.backoff.ms": 0,
        "fetch.wait.max.ms": 10,
        "session.timeout.ms": 6000,
        "statistics.interval.ms": 15000,
    }

    def __init__(
        self,
        config: dict[str, typing.Any],
        topics: typing.Iterable[str],
        schema_registry_client: SchemaRegistryClient | None = None,
        error_callback: typing.Callable | None = None,
        commit_success_callback: typing.Callable | None = None,
        commit_error_callback: typing.Callable | None = None,
        metrics: KafkaMetrics | None = None,
    ) -> None:
        self._subscribed = False
        self.topics = list(topics)
        self.non_blocking = False  # TODO
        self.timeout = 0.1  # TODO
        self._schema_registry_client = schema_registry_client

        self.error_callback = error_callback
        self.commit_success_callback = commit_success_callback
        self.commit_error_callback = commit_error_callback

        self.metrics = metrics

        self._deserializers: dict[int, AvroDeserializer | JSONDeserializer] = {}

        config = {**self.DEFAULT_CONFIG, **config}
        config["on_commit"] = self._on_commit
        config["error_cb"] = self._on_error
        config["throttle_cb"] = self._on_throttle
        config["stats_cb"] = self._on_stats

        logger.info("Initializing consumer", extra={"config": config})
        atexit.register(self._close)
        self._consumer_impl = self._init_consumer_impl(config)
        self._generator = self._message_generator()

    @staticmethod
    def _init_consumer_impl(config: dict[str, typing.Any]) -> ConfluentConsumer:
        return ConfluentConsumer(
            config,  # logger=logging.getLogger("librdkafka.consumer")
        )

    def _subscribe(self) -> None:
        if self._subscribed:
            return
        self._consumer_impl.subscribe(self.topics)
        self._subscribed = True

    def __iter__(self) -> "Consumer":
        self._subscribe()
        return self

    def __next__(self) -> Message | None:
        self._subscribe()
        try:
            return next(self._generator)
        except:
            self._close()
            raise

    def __enter__(self) -> "Consumer":
        self._consumer_impl.subscribe(self.topics)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._close()

    def _close(self) -> None:
        """Close down the consumer cleanly accordingly:
        - stops consuming
        - commit offsets (only on auto commit)
        - leave consumer group
        """
        logger.info("Closing consumer")
        try:  # noqa: SIM105
            self._consumer_impl.close()
        except RuntimeError:
            # Consumer is probably already closed
            pass

    def _poll(self) -> ConfluentMessage:
        return self._consumer_impl.poll(timeout=self.timeout)

    def _message_generator(self) -> typing.Generator[Message | None, None, None]:
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
            yield self._deserialize(message)

    def _get_schema_id(self, data: bytes) -> int:
        with _ContextStringIO(data) as payload:
            magic, schema_id = struct.unpack(">bI", payload.read(5))
            return schema_id if magic == _MAGIC_BYTE else None

    def _try_deserialize(
        self, data: bytes, topic: str, is_key: bool = False
    ) -> None | bytes | dict:
        if data is None:
            return None

        schema_id = self._get_schema_id(data)
        # TODO: the following is WRONG if it's not an avro message
        if schema_id is None:
            return data

        ser_context = SerializationContext(
            topic,
            MessageField.KEY if is_key else MessageField.VALUE,
        )

        deserializer = self._deserializers.get(schema_id)
        if deserializer:
            return deserializer(data, ser_context)

        for deserializer_cls in [AvroDeserializer, JSONDeserializer]:
            deserializer = deserializer_cls(self._schema_registry_client)
            try:
                deserialized = deserializer(data, ser_context)
                self._deserializers[schema_id] = deserialized
                return deserialized
            except Exception as e:
                logger.debug(
                    "Deserializer failed to deserialize message",
                    extra={"deserializer": deserializer, "error_message": str(e)},
                )
        # We can't tell if the data is a string or just bytes, so we return it as is
        # return data.decode("utf-8")
        return data

    def _deserialize(self, message: ConfluentMessage) -> Message:
        return Message(
            message,
            self._try_deserialize(message.key(), message.topic(), is_key=True),
            self._try_deserialize(message.value(), message.topic(), is_key=False),
        )

    def commit(
        self, message: Message | None = None, sync: bool = True
    ) -> list[TopicPartition] | None:
        """Commits current consumer offsets.

        :param message: message to commit offset for. If None, commits all offsets: use with caution
        :param sync: do a synchronous commit (true by default)
        """
        if not message:
            return self._consumer_impl.commit(asynchronous=not sync)
        return self._consumer_impl.commit(message.message, asynchronous=not sync)

    def _on_commit(self, err: KafkaError, topics_partitions) -> None:
        if err:
            logger.warning("Offset commit failed", extra={"error_message": str(err)})
            if self.commit_error_callback:
                self.commit_error_callback(topics_partitions, err)
        else:
            logger.debug(
                "Offset commit succeeded",
                extra={"topics_partitions": topics_partitions},
            )
            if self.commit_success_callback:
                self.commit_success_callback(topics_partitions)

    def _on_error(self, error: KafkaError) -> None:
        logger.error(
            error.str(), extra={"error_code": error.code(), "error_name": error.name()}
        )
        if self.error_callback:
            self.error_callback(error)

    def _on_throttle(self, event) -> None:
        logger.warning("Throttle", extra={"throttle_event": event})

    def _on_stats(self, stats: str) -> None:
        if self.metrics:
            self.metrics.send(stats)

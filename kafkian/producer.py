import atexit
import json
import logging
import socket
import typing
from enum import Enum

from confluent_kafka.cimpl import KafkaError
from confluent_kafka.cimpl import Producer as ConfluentProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from pydantic import BaseModel

try:
    from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
except ImportError:
    ProtobufSerializer = None

from confluent_kafka.serialization import MessageField, SerializationContext

from kafkian.serde.serialization import SubjectNameStrategy

logger = logging.getLogger(__name__)


class SerializationFormat(Enum):
    AVRO = "avro"
    JSON = "json"
    PROTOBUF = "protobuf"


class PydanticKafkaMessageBaseMixin:
    class Config:
        serialization_format: SerializationFormat = None  # SerializationFormat.AVRO
        subject_name_strategy: SubjectNameStrategy = (
            SubjectNameStrategy.RecordNameStrategy
        )


class _PydanticKafkaMessage(BaseModel, PydanticKafkaMessageBaseMixin):
    """This class is used only for type hinting"""

    pass


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
        schema_registry_client: SchemaRegistryClient | None = None,
        error_callback: typing.Callable | None = None,
        delivery_success_callback: typing.Callable | None = None,
        delivery_error_callback: typing.Callable | None = None,
        metrics=None,
    ) -> None:
        self._schema_registry_client = schema_registry_client

        self._serializers: dict[
            str, AvroSerializer | JSONSerializer | ProtobufSerializer
        ] = {}

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
        key: typing.AnyStr | _PydanticKafkaMessage | None,
        value: typing.AnyStr | _PydanticKafkaMessage | None,
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
        serialized_key = self._serialize(key, topic, is_key=True)
        # If value is None, it's a "tombstone" and shall be passed through
        serialized_value = self._serialize(value, topic) if value is not None else None
        headers = headers or {}
        self._produce(topic, serialized_key, serialized_value, headers)
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
            error.str(),
            extra={
                "error_code": error.code(),
                "error_name": error.name(),
            },
        )
        if self.error_callback:
            self.error_callback(error)

    def _on_throttle(self, event) -> None:
        logger.warning("Throttle", extra={"throttle_event": event})

    def _on_stats(self, stats):
        if self.metrics:
            self.metrics.send(stats)

    def _serialize(self, value, topic: str, is_key: bool = False) -> bytes:
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode("utf-8")
        elif isinstance(value, int | float):
            return str(value).encode("utf-8")
        elif isinstance(value, PydanticKafkaMessageBaseMixin) and isinstance(
            value, BaseModel
        ):
            return self._serialize_pydantic(value, topic, is_key)
        else:
            raise ValueError(f"Unsupported value type: {type(value)}")

    def _serialize_pydantic(
        self, value: _PydanticKafkaMessage, topic: str, is_key: bool = False
    ) -> bytes:
        if value.Config.serialization_format == SerializationFormat.AVRO:
            schema = value.Config.avro_schema
            schema_str = json.dumps(schema.to_json())
            if schema_str not in self._serializers:
                self._serializers[schema_str] = AvroSerializer(
                    self._schema_registry_client,
                    schema_str=schema_str,
                    conf={
                        "auto.register.schemas": True,
                        "subject.name.strategy": value.Config.subject_name_strategy,
                    },
                )
        elif value.Config.serialization_format == SerializationFormat.JSON:
            schema_str = json.dumps(value.model_json_schema(mode="serialization"))
            if schema_str not in self._serializers:
                self._serializers[schema_str] = JSONSerializer(
                    schema_str,
                    self._schema_registry_client,
                    conf={
                        "auto.register.schemas": True,
                        "subject.name.strategy": value.Config.subject_name_strategy,
                    },
                )
        elif value.Config.serialization_format == SerializationFormat.PROTOBUF:
            if ProtobufSerializer is None:
                raise ValueError("Protobuf serialization is not available")
            schema_str = value.Config.protobuf_schema
            if schema_str not in self._serializers:
                self._serializers[schema_str] = ProtobufSerializer(
                    schema_str,
                    self._schema_registry_client,
                    conf={
                        "auto.register.schemas": True,
                        "subject.name.strategy": value.Config.subject_name_strategy,
                    },
                )
        else:
            raise ValueError(
                f"Unsupported serialization format: {value.Config.serialization_format}"
            )

        serializer = self._serializers[schema_str]

        ser_context = SerializationContext(
            topic,
            MessageField.KEY if is_key else MessageField.VALUE,
        )

        return serializer(value.model_dump(mode="json"), ser_context)

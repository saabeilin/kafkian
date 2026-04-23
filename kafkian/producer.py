from __future__ import annotations

from collections.abc import Callable
from typing import Any

from confluent_kafka import KafkaException, Producer
from confluent_kafka import Message as CMessage
from confluent_kafka import TIMESTAMP_NOT_AVAILABLE
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from kafkian.base import AvroModel, Message
from kafkian.schema_registry import SchemaRegistry


class KafkianProducer:
    """Confluent Kafka producer that serialises AvroModel instances via Schema Registry.

    Accepts an injected ``SchemaRegistry`` so callers control SR configuration.
    On the first ``produce()`` call for each model class, dependencies are
    auto-registered in topological order via ``SchemaRegistry.ensure_registered``.
    ``AvroSerializer`` instances are then created and cached per model class.

    Usage::

        sr = SchemaRegistry(SchemaRegistryClient({"url": "http://localhost:8081"}))
        sr.register_model(AuditModel)
        sr.register_model(OrderCreatedModel)

        producer = KafkianProducer(
            Producer({"bootstrap.servers": "localhost:9092"}),
            sr,
        )
        producer.produce("orders", order_model, key="order-123")
        producer.flush()
    """

    def __init__(
        self,
        producer: Producer,
        schema_registry: SchemaRegistry,
        serializer_conf: dict[str, Any] | None = None,
    ) -> None:
        self._producer = producer
        self._schema_registry = schema_registry
        self._sr_client = schema_registry.client
        self._serializer_conf: dict[str, Any] = serializer_conf or {}
        self._serializers: dict[type[AvroModel], AvroSerializer] = {}
        self._key_serializer = StringSerializer("utf_8")

    def produce(
        self,
        topic: str,
        value: AvroModel,
        *,
        key: str | bytes | None = None,
        partition: int | None = None,
        on_delivery: Callable[..., None] | None = None,
        headers: dict[str, str] | list[tuple[str, str]] | None = None,
        wait: bool = True,
    ) -> Message | None:
        """Serialise *value* and produce it to *topic*.

        Args:
            topic: Destination Kafka topic.
            value: An AvroModel instance; its class's ``_schema`` is used to
                   locate or register the schema in Schema Registry.
            key: Message key.  Strings are UTF-8 serialised; bytes are passed
                 through unchanged; ``None`` produces a keyless message.
            partition: Target partition.  Omit to use the configured partitioner.
            on_delivery: Delivery callback ``(KafkaError | None, Message) -> None``.
            headers: Message headers.
            wait: Block until the broker acknowledges delivery and return a
                  ``Message``.  When ``False``, returns ``None`` immediately
                  after enqueuing; call :meth:`flush` or :meth:`poll` later.
        """
        serializer = self._get_serializer(type(value))
        serialized_value = serializer(
            value, SerializationContext(topic, MessageField.VALUE)
        )

        serialized_key: bytes | None
        match key:
            case str():
                serialized_key = self._key_serializer(
                    key, SerializationContext(topic, MessageField.KEY)
                )
            case bytes() | None:
                serialized_key = key

        kwargs: dict[str, Any] = {
            "topic": topic,
            "value": serialized_value,
            "key": serialized_key,
        }
        if partition is not None:
            kwargs["partition"] = partition
        if headers is not None:
            kwargs["headers"] = headers

        if wait:
            delivered: list[CMessage] = []

            def _delivery_cb(err: Any, msg: CMessage) -> None:
                if on_delivery is not None:
                    on_delivery(err, msg)
                delivered.append(msg)

            kwargs["on_delivery"] = _delivery_cb
        elif on_delivery is not None:
            kwargs["on_delivery"] = on_delivery

        self._producer.produce(**kwargs)

        if wait:
            while not delivered:
                self._producer.poll(0.1)
            msg = delivered[0]
            if msg.error():
                raise KafkaException(msg.error())
            return self._to_message(msg, value=value, key=key)
        return None

    def _to_message(
        self,
        msg: CMessage,
        *,
        value: AvroModel,
        key: str | bytes | None,
    ) -> Message:
        ts_type, ts_ms = msg.timestamp()
        raw_headers = msg.headers()
        return Message(
            topic=msg.topic(),
            value=value,
            key=key.decode("utf-8") if isinstance(key, bytes) else key,
            headers=(
                {
                    k: v.decode("utf-8") if isinstance(v, bytes) else v
                    for k, v in raw_headers
                }
                if raw_headers
                else None
            ),
            timestamp_ms=ts_ms if ts_type != TIMESTAMP_NOT_AVAILABLE else None,
            partition=msg.partition(),
            offset=msg.offset(),
        )

    def flush(self, timeout: float = -1) -> int:
        """Block until all pending messages are delivered. Returns remaining message count."""
        return self._producer.flush(timeout)

    def poll(self, timeout: float = 0) -> int:
        """Poll for delivery callbacks. Returns number of events served."""
        return self._producer.poll(timeout)

    def _get_serializer(self, model_cls: type[AvroModel]) -> AvroSerializer:
        if model_cls not in self._serializers:
            self._schema_registry.ensure_registered(model_cls)
            self._serializers[model_cls] = AvroSerializer(
                schema_registry_client=self._sr_client,
                schema_str=self._schema_registry.build_schema(model_cls),
                to_dict=lambda obj, _ctx: obj.model_dump(),
                conf=self._serializer_conf or None,
            )
        return self._serializers[model_cls]

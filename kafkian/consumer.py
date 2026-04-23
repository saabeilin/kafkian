from __future__ import annotations

import logging
from collections.abc import Iterator
from enum import Enum
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka import Message as CMessage
from confluent_kafka import TIMESTAMP_NOT_AVAILABLE
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
)

from kafkian.base import AvroModel, Message
from kafkian.schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


class _SkipSentinel:
    """Typed sentinel returned by _handle_unknown to signal a message should be skipped."""


_SKIP = _SkipSentinel()


class UnknownSchema(str, Enum):
    RAISE = "raise"
    SKIP = "skip"
    RAW = "raw"


class KafkianConsumer:
    """Confluent Kafka consumer that deserialises Avro messages into AvroModel instances.

    Accepts an injected ``SchemaRegistry`` so callers control SR configuration.
    Models must be indexed via ``SchemaRegistry.register_model()`` before consuming
    so that record names from the wire can be mapped to Python classes.

    The ``unknown_schemas`` parameter controls behaviour when a record name has no
    registered model: ``"raise"`` (default) raises ``LookupError``, ``"skip"`` logs
    a warning and silently drops the message, ``"raw"`` yields it with the value as
    a ``dict`` (for Avro messages) or ``bytes`` (for non-Avro messages).

    Usage::

        sr = SchemaRegistry(SchemaRegistryClient({"url": "http://localhost:8081"}))
        sr.register_model(OrderCreatedModel)

        raw_consumer = Consumer({"bootstrap.servers": "...", "group.id": "g1"})
        raw_consumer.subscribe(["orders"])

        with KafkianConsumer(raw_consumer, sr) as consumer:
            for message in consumer.consume():
                handle(message.value)
                consumer.commit_offset(message)
    """

    def __init__(
        self,
        consumer: Consumer,
        schema_registry: SchemaRegistry,
        unknown_schemas: UnknownSchema = UnknownSchema.RAISE,
    ) -> None:
        self._consumer = consumer
        self._schema_registry = schema_registry
        self._unknown_schemas = unknown_schemas
        self._deserializer = AvroDeserializer(
            schema_registry_client=schema_registry.client,
            return_record_name=True,
        )
        self._key_deserializer = StringDeserializer("utf_8")

    def consume(self, timeout: float = 1.0) -> Iterator[Message]:
        """Poll indefinitely and yield decoded ``Message`` objects.

        Skips ``None`` poll results (timeouts).  Raises ``KafkaException`` on
        broker errors.  Call ``commit_offset()`` to commit processed offsets.
        """
        while True:
            msg = self._consumer.poll(timeout)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            value: AvroModel | dict[str, Any] | bytes | None = None
            wire_value: bytes | None = msg.value()
            msg_topic: str = (
                msg.topic() or ""
            )  # confluent_kafka stubs return str | None
            if wire_value is not None:
                raw = self._deserializer(
                    wire_value,
                    SerializationContext(msg_topic, MessageField.VALUE),
                )
                match raw:
                    case (str() as name, dict() as data):
                        model_cls = self._schema_registry.lookup_model(name)
                        if model_cls is not None:
                            value = model_cls(**data)
                        else:
                            result = self._handle_unknown(name, data, msg)
                            if isinstance(result, _SkipSentinel):
                                continue
                            value = result
                    case dict() as data:
                        result = self._handle_unknown(None, data, msg)
                        if isinstance(result, _SkipSentinel):
                            continue
                        value = result
                    case _:
                        result = self._handle_unknown(None, wire_value, msg)
                        if isinstance(result, _SkipSentinel):
                            continue
                        value = result

            key: str | None = None
            wire_key: bytes | None = msg.key()
            if wire_key is not None:
                key = self._key_deserializer(
                    wire_key,
                    SerializationContext(msg_topic, MessageField.KEY),
                )

            yield self._to_message(msg, value=value, key=key)

    def commit_offset(
        self,
        messages: Message | list[Message | TopicPartition],
        *,
        asynchronous: bool = False,
    ) -> None:
        """Commit offsets for one or more messages.

        Accepts a single ``Message``, a list of ``Message`` objects, or a list of
        ``confluent_kafka.TopicPartition`` objects.  When deriving offsets from
        ``Message`` instances, commits ``offset + 1`` per Kafka convention.
        """
        if isinstance(messages, Message):
            messages = [messages]

        offsets: list[TopicPartition] = []
        for item in messages:
            match item:
                case Message(topic=t, partition=p, offset=o) if (
                    p is not None and o is not None
                ):
                    offsets.append(TopicPartition(t, p, o + 1))
                case TopicPartition():
                    offsets.append(item)
                case _:
                    raise ValueError(f"Cannot derive offset from {item!r}")

        self._consumer.commit(offsets=offsets, asynchronous=asynchronous)

    def close(self) -> None:
        """Close the underlying consumer, committing final offsets if configured."""
        self._consumer.close()

    def __enter__(self) -> KafkianConsumer:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _handle_unknown(
        self,
        record_name: str | None,
        raw: dict[str, Any] | bytes,
        msg: CMessage,
    ) -> dict[str, Any] | bytes | _SkipSentinel:
        label = record_name or "<unknown>"
        match self._unknown_schemas:
            case UnknownSchema.RAISE:
                raise LookupError(
                    f"No model registered for Avro record '{label}' "
                    f"(topic={msg.topic()}, partition={msg.partition()}, "
                    f"offset={msg.offset()}). "
                    "Call schema_registry.register_model() before consuming."
                )
            case UnknownSchema.SKIP:
                logger.warning(
                    "Skipping message with unregistered schema '%s' "
                    "(topic=%s partition=%d offset=%d)",
                    label,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
                return _SKIP
            case UnknownSchema.RAW:
                return raw
            case _:
                raise AssertionError(
                    f"Unhandled UnknownSchema variant: {self._unknown_schemas}"
                )

    def _to_message(
        self,
        msg: CMessage,
        *,
        value: AvroModel | dict[str, Any] | bytes | None,
        key: str | None,
    ) -> Message:
        ts_type, ts_ms = msg.timestamp()
        raw_headers = msg.headers()
        return Message(
            topic=msg.topic(),
            value=value,
            key=key,
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

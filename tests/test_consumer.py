from __future__ import annotations

import logging
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, TopicPartition
from kafkian.base import AvroModel, Message
from kafkian.consumer import KafkianConsumer, UnknownSchema


class OrderModel(AvroModel):
    _schema: ClassVar[dict[str, Any]] = {
        "type": "record",
        "name": "Order",
        "fields": [{"name": "order_id", "type": "string"}],
    }
    order_id: str


class EventModel(AvroModel):
    _schema: ClassVar[dict[str, Any]] = {
        "type": "record",
        "name": "Event",
        "fields": [{"name": "name", "type": "string"}],
    }
    name: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_msg(
    topic: str = "t",
    value: bytes | None = b"v",
    key: bytes | None = None,
    headers: list[tuple[str, bytes]] | None = None,
    partition: int = 0,
    offset: int = 5,
    error: Any = None,
    ts_type: int = TIMESTAMP_NOT_AVAILABLE,
    ts_ms: int = 0,
) -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = error
    msg.topic.return_value = topic
    msg.value.return_value = value
    msg.key.return_value = key
    msg.headers.return_value = headers
    msg.timestamp.return_value = (ts_type, ts_ms)
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    return msg


def _consumer_with_messages(
    msgs: list[Any],
    schema_registry: MagicMock | None = None,
    unknown_schemas: UnknownSchema = UnknownSchema.RAISE,
) -> tuple[KafkianConsumer, MagicMock]:
    """Build a KafkianConsumer whose underlying Consumer.poll() returns *msgs* then stalls."""
    raw_consumer = MagicMock()
    # After msgs are exhausted, poll returns None (simulate no more messages)
    raw_consumer.poll.side_effect = msgs + [None, None, None]

    sr = schema_registry or MagicMock()
    sr.client = MagicMock()

    consumer = KafkianConsumer(raw_consumer, sr, unknown_schemas=unknown_schemas)
    return consumer, raw_consumer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_sr() -> MagicMock:
    sr = MagicMock()
    sr.client = MagicMock()
    sr.lookup_model.return_value = OrderModel
    return sr


@pytest.fixture
def mock_deserializer() -> MagicMock:
    d = MagicMock()
    d.return_value = ("Order", {"order_id": "abc"})
    return d


# ---------------------------------------------------------------------------
# consume() — happy path
# ---------------------------------------------------------------------------


def test_consume_yields_decoded_avro_model(mock_sr: MagicMock) -> None:
    msg = _make_mock_msg(topic="orders", value=b"wire", key=b"k1")
    raw_consumer = MagicMock()
    raw_consumer.poll.side_effect = [msg, None]

    consumer = KafkianConsumer(raw_consumer, mock_sr)
    with patch(
        "kafkian.consumer.AvroDeserializer",
        return_value=MagicMock(return_value=("Order", {"order_id": "xyz"})),
    ):
        consumer._deserializer = MagicMock(return_value=("Order", {"order_id": "xyz"}))
        consumer._key_deserializer = MagicMock(return_value="k1")

        result = next(consumer.consume())

    assert isinstance(result.value, OrderModel)
    assert result.value.order_id == "xyz"
    assert result.topic == "orders"


def test_consume_skips_none_poll_results(mock_sr: MagicMock) -> None:
    msg = _make_mock_msg(value=b"wire")
    raw_consumer = MagicMock()
    # First two polls return None (timeouts), third returns a real message
    raw_consumer.poll.side_effect = [None, None, msg, None]

    consumer = KafkianConsumer(raw_consumer, mock_sr)
    consumer._deserializer = MagicMock(return_value=("Order", {"order_id": "abc"}))
    consumer._key_deserializer = MagicMock(return_value=None)

    result = next(consumer.consume())
    assert isinstance(result.value, OrderModel)
    assert raw_consumer.poll.call_count == 3


def test_consume_raises_on_broker_error(mock_sr: MagicMock) -> None:
    from confluent_kafka import KafkaException

    err_msg = _make_mock_msg()
    err_msg.error.return_value = MagicMock()  # truthy error
    raw_consumer = MagicMock()
    raw_consumer.poll.return_value = err_msg

    consumer = KafkianConsumer(raw_consumer, mock_sr)
    with pytest.raises(KafkaException):
        next(consumer.consume())


def test_consume_handles_tombstone_value(mock_sr: MagicMock) -> None:
    msg = _make_mock_msg(value=None)
    raw_consumer = MagicMock()
    raw_consumer.poll.side_effect = [msg, None]

    consumer = KafkianConsumer(raw_consumer, mock_sr)

    result = next(consumer.consume())
    assert result.value is None


def test_consume_handles_none_key(mock_sr: MagicMock) -> None:
    msg = _make_mock_msg(value=b"wire", key=None)
    raw_consumer = MagicMock()
    raw_consumer.poll.side_effect = [msg, None]

    consumer = KafkianConsumer(raw_consumer, mock_sr)
    consumer._deserializer = MagicMock(return_value=("Order", {"order_id": "abc"}))

    result = next(consumer.consume())
    assert result.key is None


def test_consume_populates_message_metadata(mock_sr: MagicMock) -> None:
    from confluent_kafka import TIMESTAMP_CREATE_TIME

    msg = _make_mock_msg(
        topic="orders",
        partition=2,
        offset=99,
        ts_type=TIMESTAMP_CREATE_TIME,
        ts_ms=1700000000000,
        headers=[("x-trace", b"abc")],
    )
    raw_consumer = MagicMock()
    raw_consumer.poll.side_effect = [msg, None]

    consumer = KafkianConsumer(raw_consumer, mock_sr)
    consumer._deserializer = MagicMock(return_value=("Order", {"order_id": "z"}))
    consumer._key_deserializer = MagicMock(return_value=None)

    result = next(consumer.consume())
    assert result.topic == "orders"
    assert result.partition == 2
    assert result.offset == 99
    assert result.timestamp_ms == 1700000000000
    assert result.headers == {"x-trace": "abc"}


# ---------------------------------------------------------------------------
# unknown_schemas — RAISE
# ---------------------------------------------------------------------------


def test_unknown_schemas_raise_on_unregistered_record(mock_sr: MagicMock) -> None:
    mock_sr.lookup_model.return_value = None

    msg = _make_mock_msg(value=b"wire")
    raw_consumer = MagicMock()
    raw_consumer.poll.side_effect = [msg, None]

    consumer = KafkianConsumer(
        raw_consumer, mock_sr, unknown_schemas=UnknownSchema.RAISE
    )
    consumer._deserializer = MagicMock(return_value=("UnknownRecord", {"x": 1}))

    with pytest.raises(LookupError, match="UnknownRecord"):
        next(consumer.consume())


# ---------------------------------------------------------------------------
# unknown_schemas — SKIP
# ---------------------------------------------------------------------------


def test_unknown_schemas_skip_does_not_yield(
    mock_sr: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    mock_sr.lookup_model.return_value = None

    unknown_msg = _make_mock_msg(value=b"wire", topic="orders", partition=0, offset=7)
    known_msg = _make_mock_msg(value=b"wire2", topic="orders", partition=0, offset=8)
    raw_consumer = MagicMock()
    raw_consumer.poll.side_effect = [unknown_msg, known_msg, None]

    consumer = KafkianConsumer(
        raw_consumer, mock_sr, unknown_schemas=UnknownSchema.SKIP
    )
    # First call returns unknown, second returns known Order
    consumer._deserializer = MagicMock(
        side_effect=[
            ("UnknownRecord", {"x": 1}),
            ("Order", {"order_id": "found"}),
        ]
    )
    consumer._key_deserializer = MagicMock(return_value=None)
    # After the first unknown is skipped, lookup should succeed for "Order"
    mock_sr.lookup_model.side_effect = [None, OrderModel]

    with caplog.at_level(logging.WARNING, logger="kafkian.consumer"):
        result = next(consumer.consume())

    assert isinstance(result.value, OrderModel)
    assert "UnknownRecord" in caplog.text


def test_unknown_schemas_skip_logs_warning(
    mock_sr: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    mock_sr.lookup_model.return_value = None

    msg = _make_mock_msg(value=b"wire", topic="t", partition=1, offset=3)
    known = _make_mock_msg(value=None, topic="t")  # tombstone follows to end generator
    raw_consumer = MagicMock()
    raw_consumer.poll.side_effect = [msg, known, None]

    consumer = KafkianConsumer(
        raw_consumer, mock_sr, unknown_schemas=UnknownSchema.SKIP
    )
    consumer._deserializer = MagicMock(return_value=("Ghost", {"y": 2}))

    with caplog.at_level(logging.WARNING, logger="kafkian.consumer"):
        next(consumer.consume())  # yields tombstone, skips Ghost

    assert "Ghost" in caplog.text
    assert "t" in caplog.text


# ---------------------------------------------------------------------------
# unknown_schemas — RAW
# ---------------------------------------------------------------------------


def test_unknown_schemas_raw_yields_dict_for_avro(mock_sr: MagicMock) -> None:
    mock_sr.lookup_model.return_value = None

    msg = _make_mock_msg(value=b"wire")
    raw_consumer = MagicMock()
    raw_consumer.poll.side_effect = [msg, None]

    consumer = KafkianConsumer(raw_consumer, mock_sr, unknown_schemas=UnknownSchema.RAW)
    consumer._deserializer = MagicMock(return_value=("UnknownRecord", {"field": "val"}))
    consumer._key_deserializer = MagicMock(return_value=None)

    result = next(consumer.consume())
    assert result.value == {"field": "val"}


def test_unknown_schemas_raw_yields_bytes_for_non_avro(mock_sr: MagicMock) -> None:
    mock_sr.lookup_model.return_value = None

    msg = _make_mock_msg(value=b"\x01\x02\x03")
    raw_consumer = MagicMock()
    raw_consumer.poll.side_effect = [msg, None]

    consumer = KafkianConsumer(raw_consumer, mock_sr, unknown_schemas=UnknownSchema.RAW)
    # Deserializer returns non-tuple, non-dict (simulates non-Avro SR format)
    consumer._deserializer = MagicMock(return_value=b"\x01\x02\x03")
    consumer._key_deserializer = MagicMock(return_value=None)

    result = next(consumer.consume())
    assert result.value == b"\x01\x02\x03"


# ---------------------------------------------------------------------------
# commit_offset()
# ---------------------------------------------------------------------------


def test_commit_offset_single_message(mock_sr: MagicMock) -> None:
    raw_consumer = MagicMock()
    consumer = KafkianConsumer(raw_consumer, mock_sr)

    msg = Message(topic="orders", value=None, key=None, partition=1, offset=42)
    consumer.commit_offset(msg)

    raw_consumer.commit.assert_called_once()
    offsets = raw_consumer.commit.call_args.kwargs["offsets"]
    assert len(offsets) == 1
    assert offsets[0].topic == "orders"
    assert offsets[0].partition == 1
    assert offsets[0].offset == 43  # offset + 1


def test_commit_offset_list_of_messages(mock_sr: MagicMock) -> None:
    raw_consumer = MagicMock()
    consumer = KafkianConsumer(raw_consumer, mock_sr)

    msgs: list[Message | TopicPartition] = [
        Message(topic="t", value=None, key=None, partition=0, offset=10),
        Message(topic="t", value=None, key=None, partition=1, offset=20),
    ]
    consumer.commit_offset(msgs)

    offsets = raw_consumer.commit.call_args.kwargs["offsets"]
    assert len(offsets) == 2
    assert offsets[0].offset == 11
    assert offsets[1].offset == 21


def test_commit_offset_list_of_topic_partitions(mock_sr: MagicMock) -> None:
    raw_consumer = MagicMock()
    consumer = KafkianConsumer(raw_consumer, mock_sr)

    tps: list[Message | TopicPartition] = [
        TopicPartition("t", 0, 100),
        TopicPartition("t", 1, 200),
    ]
    consumer.commit_offset(tps)

    offsets = raw_consumer.commit.call_args.kwargs["offsets"]
    assert offsets == tps


def test_commit_offset_asynchronous_flag(mock_sr: MagicMock) -> None:
    raw_consumer = MagicMock()
    consumer = KafkianConsumer(raw_consumer, mock_sr)

    msg = Message(topic="t", value=None, key=None, partition=0, offset=5)
    consumer.commit_offset(msg, asynchronous=True)

    raw_consumer.commit.assert_called_once_with(
        offsets=[TopicPartition("t", 0, 6)], asynchronous=True
    )


def test_commit_offset_invalid_raises(mock_sr: MagicMock) -> None:
    raw_consumer = MagicMock()
    consumer = KafkianConsumer(raw_consumer, mock_sr)

    with pytest.raises(ValueError):
        consumer.commit_offset(["not-a-message"])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Context manager / close()
# ---------------------------------------------------------------------------


def test_context_manager_calls_close(mock_sr: MagicMock) -> None:
    raw_consumer = MagicMock()
    consumer = KafkianConsumer(raw_consumer, mock_sr)

    with consumer:
        pass

    raw_consumer.close.assert_called_once()


def test_close_calls_consumer_close(mock_sr: MagicMock) -> None:
    raw_consumer = MagicMock()
    consumer = KafkianConsumer(raw_consumer, mock_sr)
    consumer.close()
    raw_consumer.close.assert_called_once()

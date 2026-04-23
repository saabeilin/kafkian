from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from confluent_kafka import TIMESTAMP_NOT_AVAILABLE
from kafkian.base import AvroModel, Message
from kafkian.producer import KafkianProducer


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
# Fixtures
# ---------------------------------------------------------------------------


def _make_mock_msg(
    topic: str = "t",
    value: bytes = b"v",
    key: bytes | None = None,
    headers: list[tuple[str, bytes]] | None = None,
    partition: int = 0,
    offset: int = 0,
    error: Any = None,
) -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = error
    msg.topic.return_value = topic
    msg.value.return_value = value
    msg.key.return_value = key
    msg.headers.return_value = headers
    msg.timestamp.return_value = (TIMESTAMP_NOT_AVAILABLE, 0)
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    return msg


@pytest.fixture
def mock_producer() -> MagicMock:
    mp = MagicMock()
    pending: list[Any] = []

    def fake_produce(**kwargs: Any) -> None:
        cb = kwargs.get("on_delivery")
        if cb:
            pending.append(cb)

    def fake_poll(timeout: float = 0) -> int:
        while pending:
            cb = pending.pop(0)
            cb(None, _make_mock_msg())
        return 0

    mp.produce.side_effect = fake_produce
    mp.poll.side_effect = fake_poll
    return mp


@pytest.fixture
def mock_sr() -> MagicMock:
    sr = MagicMock()
    sr.client = MagicMock()
    sr.build_schema.return_value = MagicMock()
    return sr


@pytest.fixture
def mock_avro_serializer() -> MagicMock:
    s = MagicMock()
    s.return_value = b"serialized-value"
    return s


@pytest.fixture
def kafkian_producer(mock_producer: MagicMock, mock_sr: MagicMock) -> KafkianProducer:
    return KafkianProducer(mock_producer, mock_sr)


# ---------------------------------------------------------------------------
# produce() — value serialisation
# ---------------------------------------------------------------------------


def test_produce_serialises_value(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        order = OrderModel(order_id="abc")
        kafkian_producer.produce("orders", order)

    kwargs = mock_producer.produce.call_args.kwargs
    assert kwargs["topic"] == "orders"
    assert kwargs["value"] == b"serialized-value"


def test_produce_passes_model_dump_to_serializer(
    kafkian_producer: KafkianProducer,
    mock_avro_serializer: MagicMock,
) -> None:
    """to_dict callback must receive the AvroModel and call model_dump()."""
    captured: list[object] = []

    def capturing_serializer(obj: object, ctx: object) -> bytes:
        captured.append(obj)
        return b"x"

    mock_avro_serializer.side_effect = capturing_serializer

    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        order = OrderModel(order_id="xyz")
        kafkian_producer.produce("orders", order)

    assert captured[0] is order


# ---------------------------------------------------------------------------
# produce() — key handling
# ---------------------------------------------------------------------------


def test_produce_str_key_is_serialised(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("orders", OrderModel(order_id="1"), key="my-key")

    serialized_key = mock_producer.produce.call_args.kwargs["key"]
    assert isinstance(serialized_key, bytes)
    assert serialized_key == b"my-key"


def test_produce_bytes_key_is_passed_through(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("orders", OrderModel(order_id="1"), key=b"\x00\x01")

    assert mock_producer.produce.call_args.kwargs["key"] == b"\x00\x01"


def test_produce_none_key(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("orders", OrderModel(order_id="1"))

    assert mock_producer.produce.call_args.kwargs["key"] is None


# ---------------------------------------------------------------------------
# produce() — optional kwargs forwarded only when provided
# ---------------------------------------------------------------------------


def test_produce_optional_kwargs_not_forwarded_by_default(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("t", OrderModel(order_id="1"))

    kwargs = mock_producer.produce.call_args.kwargs
    assert "partition" not in kwargs
    assert "headers" not in kwargs
    # on_delivery is always present when wait=True (internal delivery callback)


def test_produce_forwards_on_delivery(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    cb = MagicMock()
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("t", OrderModel(order_id="1"), on_delivery=cb)

    # wait=True wraps on_delivery in an internal callback; the user cb is still invoked
    cb.assert_called_once()


def test_produce_forwards_partition(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("t", OrderModel(order_id="1"), partition=2)

    assert mock_producer.produce.call_args.kwargs["partition"] == 2


def test_produce_forwards_headers(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    hdrs = {"trace-id": "abc"}
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("t", OrderModel(order_id="1"), headers=hdrs)

    assert mock_producer.produce.call_args.kwargs["headers"] is hdrs


# ---------------------------------------------------------------------------
# Serializer caching
# ---------------------------------------------------------------------------


def test_serializer_cached_per_model_class(
    kafkian_producer: KafkianProducer,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch(
        "kafkian.producer.AvroSerializer", return_value=mock_avro_serializer
    ) as cls:
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("t", OrderModel(order_id="1"))
        kafkian_producer.produce("t", OrderModel(order_id="2"))

    assert cls.call_count == 1  # AvroSerializer constructed only once


def test_different_model_classes_get_separate_serializers(
    kafkian_producer: KafkianProducer,
) -> None:
    serializer_a, serializer_b = (
        MagicMock(return_value=b"a"),
        MagicMock(return_value=b"b"),
    )

    with patch(
        "kafkian.producer.AvroSerializer", side_effect=[serializer_a, serializer_b]
    ):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("t", OrderModel(order_id="1"))
        kafkian_producer.produce("t", EventModel(name="click"))

    assert len(kafkian_producer._serializers) == 2


def test_serializer_uses_schema_from_registry(
    kafkian_producer: KafkianProducer,
    mock_sr: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    mock_schema = MagicMock()
    mock_sr.build_schema.return_value = mock_schema

    with patch(
        "kafkian.producer.AvroSerializer", return_value=mock_avro_serializer
    ) as cls:
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("t", OrderModel(order_id="1"))

    assert cls.call_args.kwargs["schema_str"] is mock_schema


# ---------------------------------------------------------------------------
# ensure_registered called on first produce
# ---------------------------------------------------------------------------


def test_ensure_registered_called_on_first_produce(
    kafkian_producer: KafkianProducer,
    mock_sr: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("t", OrderModel(order_id="1"))

    mock_sr.ensure_registered.assert_called_once_with(OrderModel)


def test_ensure_registered_not_called_again_after_cache(
    kafkian_producer: KafkianProducer,
    mock_sr: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("t", OrderModel(order_id="1"))
        kafkian_producer.produce("t", OrderModel(order_id="2"))

    mock_sr.ensure_registered.assert_called_once_with(OrderModel)


# ---------------------------------------------------------------------------
# flush / poll delegation
# ---------------------------------------------------------------------------


def test_flush_delegates(
    kafkian_producer: KafkianProducer, mock_producer: MagicMock
) -> None:
    mock_producer.flush.return_value = 0
    result = kafkian_producer.flush(5.0)
    mock_producer.flush.assert_called_once_with(5.0)
    assert result == 0


def test_poll_delegates(
    kafkian_producer: KafkianProducer, mock_producer: MagicMock
) -> None:
    mock_producer.poll.side_effect = None
    mock_producer.poll.return_value = 3
    result = kafkian_producer.poll(1.0)
    mock_producer.poll.assert_called_once_with(1.0)
    assert result == 3


# ---------------------------------------------------------------------------
# wait parameter
# ---------------------------------------------------------------------------


def test_produce_wait_true_returns_message(
    kafkian_producer: KafkianProducer,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        result = kafkian_producer.produce("orders", OrderModel(order_id="1"), wait=True)

    assert isinstance(result, Message)
    assert result.topic == "t"


def test_produce_wait_false_returns_none(
    kafkian_producer: KafkianProducer,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        result = kafkian_producer.produce(
            "orders", OrderModel(order_id="1"), wait=False
        )

    assert result is None


def test_produce_wait_false_does_not_forward_internal_on_delivery(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce("t", OrderModel(order_id="1"), wait=False)

    kwargs = mock_producer.produce.call_args.kwargs
    assert "on_delivery" not in kwargs


def test_produce_wait_false_forwards_user_on_delivery(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    cb = MagicMock()
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        kafkian_producer.produce(
            "t", OrderModel(order_id="1"), on_delivery=cb, wait=False
        )

    assert mock_producer.produce.call_args.kwargs["on_delivery"] is cb


def test_produce_wait_raises_on_delivery_error(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    from confluent_kafka import KafkaException

    error_msg = _make_mock_msg(error=MagicMock())
    pending: list[Any] = []

    def fake_produce_err(**kwargs: Any) -> None:
        cb = kwargs.get("on_delivery")
        if cb:
            pending.append(cb)

    def fake_poll_err(timeout: float = 0) -> int:
        while pending:
            cb = pending.pop(0)
            cb(error_msg.error(), error_msg)
        return 0

    mock_producer.produce.side_effect = fake_produce_err
    mock_producer.poll.side_effect = fake_poll_err

    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        with pytest.raises(KafkaException):
            kafkian_producer.produce("t", OrderModel(order_id="1"), wait=True)


def test_produce_message_fields(
    kafkian_producer: KafkianProducer,
    mock_producer: MagicMock,
    mock_avro_serializer: MagicMock,
) -> None:
    pending: list[Any] = []

    def fake_produce_fields(**kwargs: Any) -> None:
        cb = kwargs.get("on_delivery")
        if cb:
            pending.append(cb)

    def fake_poll_fields(timeout: float = 0) -> int:
        while pending:
            cb = pending.pop(0)
            msg = _make_mock_msg(
                topic="orders",
                value=b"bytes",
                key=b"my-key",
                headers=[("trace-id", b"abc")],
                partition=2,
                offset=42,
            )
            msg.timestamp.return_value = (1, 1700000000000)
            cb(None, msg)
        return 0

    mock_producer.produce.side_effect = fake_produce_fields
    mock_producer.poll.side_effect = fake_poll_fields

    order = OrderModel(order_id="1")
    with patch("kafkian.producer.AvroSerializer", return_value=mock_avro_serializer):
        kafkian_producer._serializers.clear()
        result = kafkian_producer.produce("orders", order, key="my-key", wait=True)

    assert result is not None
    assert result.topic == "orders"
    assert result.value is order
    assert result.key == "my-key"
    assert result.headers == {"trace-id": "abc"}
    assert result.timestamp_ms == 1700000000000
    assert result.partition == 2
    assert result.offset == 42

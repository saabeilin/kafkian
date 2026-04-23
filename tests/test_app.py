from __future__ import annotations

import logging
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest

from kafkian.app import KafkianApp, Router, _score_pattern
from kafkian.base import AvroModel, Message
from kafkian.consumer import KafkianConsumer


class OrderModel(AvroModel):
    _schema: ClassVar[dict[str, Any]] = {
        "type": "record",
        "name": "Order",
        "fields": [{"name": "order_id", "type": "string"}],
    }
    order_id: str


class PaymentModel(AvroModel):
    _schema: ClassVar[dict[str, Any]] = {
        "type": "record",
        "name": "Payment",
        "fields": [{"name": "amount", "type": "int"}],
    }
    amount: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_message(
    topic: str = "orders",
    value: AvroModel | bytes | None = None,
    key: str | None = None,
    partition: int = 0,
    offset: int = 5,
) -> Message:
    return Message(
        topic=topic, value=value, key=key, partition=partition, offset=offset
    )


def _noop(message: Message) -> None:
    pass


# ---------------------------------------------------------------------------
# _score_pattern
# ---------------------------------------------------------------------------


def test_score_exact_topic_exact_schema() -> None:
    assert _score_pattern("orders", OrderModel) == 4


def test_score_exact_topic_no_schema() -> None:
    assert _score_pattern("orders", None) == 3


def test_score_wildcard_topic_exact_schema() -> None:
    assert _score_pattern("events.*", OrderModel) == 2


def test_score_wildcard_topic_no_schema() -> None:
    assert _score_pattern("events.*", None) == 1


# ---------------------------------------------------------------------------
# Router — registration
# ---------------------------------------------------------------------------


def test_on_registers_route() -> None:
    router = Router()

    @router.on("orders", OrderModel)
    def handler(message: Message) -> None:
        pass

    assert len(router._routes) == 1
    entry = router._routes[0]
    assert entry.topic_pattern == "orders"
    assert entry.schema_type is OrderModel
    assert entry.handler is handler
    assert entry.score == 4


def test_on_returns_original_handler() -> None:
    router = Router()

    @router.on("orders")
    def handler(message: Message) -> None:
        pass

    assert callable(handler)


def test_on_multiple_routes_appended_in_order() -> None:
    router = Router()

    @router.on("orders")
    def h1(message: Message) -> None:
        pass

    @router.on("payments")
    def h2(message: Message) -> None:
        pass

    assert [e.topic_pattern for e in router._routes] == ["orders", "payments"]


# ---------------------------------------------------------------------------
# Router — dispatch
# ---------------------------------------------------------------------------


def test_dispatch_exact_match() -> None:
    router = Router()

    @router.on("orders", OrderModel)
    def handler(message: Message) -> None:
        pass

    msg = _make_message("orders", OrderModel(order_id="x"))
    assert router.dispatch(msg) is handler


def test_dispatch_wildcard_topic_matches() -> None:
    router = Router()

    @router.on("events.*")
    def handler(message: Message) -> None:
        pass

    msg = _make_message("events.orders")
    assert router.dispatch(msg) is handler


def test_dispatch_wildcard_topic_no_match() -> None:
    router = Router()

    @router.on("events.*")
    def handler(message: Message) -> None:
        pass

    msg = _make_message("orders")
    assert router.dispatch(msg) is None


def test_dispatch_none_schema_matches_any_value() -> None:
    router = Router()

    @router.on("orders")
    def handler(message: Message) -> None:
        pass

    msg = _make_message("orders", OrderModel(order_id="x"))
    assert router.dispatch(msg) is handler

    msg2 = _make_message("orders", PaymentModel(amount=99))
    assert router.dispatch(msg2) is handler


def test_dispatch_schema_type_must_match() -> None:
    router = Router()

    @router.on("orders", OrderModel)
    def handler(message: Message) -> None:
        pass

    msg = _make_message("orders", PaymentModel(amount=10))
    assert router.dispatch(msg) is None


def test_dispatch_no_match_returns_none() -> None:
    router = Router()

    @router.on("payments", OrderModel)
    def handler(message: Message) -> None:
        pass

    msg = _make_message("orders", OrderModel(order_id="x"))
    assert router.dispatch(msg) is None


def test_dispatch_specificity_exact_topic_beats_wildcard_topic() -> None:
    router = Router()

    @router.on("orders.*")
    def wildcard_handler(message: Message) -> None:
        pass

    @router.on("orders.created", OrderModel)
    def exact_handler(message: Message) -> None:
        pass

    msg = _make_message("orders.created", OrderModel(order_id="x"))
    assert router.dispatch(msg) is exact_handler


def test_dispatch_specificity_exact_schema_beats_none() -> None:
    router = Router()

    @router.on("orders")
    def none_schema_handler(message: Message) -> None:
        pass

    @router.on("orders", OrderModel)
    def typed_handler(message: Message) -> None:
        pass

    msg = _make_message("orders", OrderModel(order_id="x"))
    assert router.dispatch(msg) is typed_handler


def test_dispatch_first_registered_wins_on_tie() -> None:
    router = Router()

    @router.on("orders", OrderModel)
    def first_handler(message: Message) -> None:
        pass

    @router.on("orders", OrderModel)
    def second_handler(message: Message) -> None:
        pass

    msg = _make_message("orders", OrderModel(order_id="x"))
    assert router.dispatch(msg) is first_handler


# ---------------------------------------------------------------------------
# Router — include_router
# ---------------------------------------------------------------------------


def test_include_router_merges_routes() -> None:
    app_router = Router()
    sub_router = Router()

    @app_router.on("orders")
    def h1(message: Message) -> None:
        pass

    @sub_router.on("payments")
    def h2(message: Message) -> None:
        pass

    app_router.include_router(sub_router)

    assert len(app_router._routes) == 2
    assert app_router._routes[1].topic_pattern == "payments"


def test_include_router_merged_routes_are_dispatched() -> None:
    app_router = Router()
    sub_router = Router()

    @sub_router.on("payments", PaymentModel)
    def handler(message: Message) -> None:
        pass

    app_router.include_router(sub_router)
    msg = _make_message("payments", PaymentModel(amount=50))
    assert app_router.dispatch(msg) is handler


# ---------------------------------------------------------------------------
# KafkianApp — fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_consumer() -> MagicMock:
    return MagicMock(spec=KafkianConsumer)


# ---------------------------------------------------------------------------
# KafkianApp — run / dispatch
# ---------------------------------------------------------------------------


def test_run_dispatches_to_handler(mock_consumer: MagicMock) -> None:
    msg = _make_message("orders", OrderModel(order_id="x"))
    mock_consumer.consume.return_value = iter([msg])

    app = KafkianApp(mock_consumer, auto_commit=False)
    called_with: list[Message] = []

    @app.on("orders", OrderModel)
    def handler(message: Message) -> None:
        called_with.append(message)

    app.run()

    assert called_with == [msg]


def test_run_commits_after_handler_success(mock_consumer: MagicMock) -> None:
    msg = _make_message("orders", OrderModel(order_id="x"))
    mock_consumer.consume.return_value = iter([msg])

    app = KafkianApp(mock_consumer, auto_commit=True)

    @app.on("orders")
    def handler(message: Message) -> None:
        pass

    app.run()

    mock_consumer.commit_offset.assert_called_once_with(msg)


def test_run_no_commit_when_auto_commit_false(mock_consumer: MagicMock) -> None:
    msg = _make_message("orders", OrderModel(order_id="x"))
    mock_consumer.consume.return_value = iter([msg])

    app = KafkianApp(mock_consumer, auto_commit=False)

    @app.on("orders")
    def handler(message: Message) -> None:
        pass

    app.run()

    mock_consumer.commit_offset.assert_not_called()


# ---------------------------------------------------------------------------
# KafkianApp — unhandled
# ---------------------------------------------------------------------------


def test_run_calls_custom_on_unhandled(mock_consumer: MagicMock) -> None:
    msg = _make_message("unknown-topic")
    mock_consumer.consume.return_value = iter([msg])

    unhandled: list[Message] = []
    app = KafkianApp(mock_consumer, on_unhandled=lambda m: unhandled.append(m))

    app.run()

    assert unhandled == [msg]
    mock_consumer.commit_offset.assert_not_called()


def test_run_default_on_unhandled_logs_warning(
    mock_consumer: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    msg = _make_message("unknown-topic")
    mock_consumer.consume.return_value = iter([msg])

    app = KafkianApp(mock_consumer)

    with caplog.at_level(logging.WARNING, logger="kafkian.app"):
        app.run()

    assert any("No handler" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# KafkianApp — error handling
# ---------------------------------------------------------------------------


def test_run_calls_custom_on_error(mock_consumer: MagicMock) -> None:
    msg = _make_message("orders", OrderModel(order_id="x"))
    mock_consumer.consume.return_value = iter([msg])

    boom = RuntimeError("boom")
    errors: list[tuple[Message, Exception]] = []
    app = KafkianApp(
        mock_consumer,
        on_error=lambda m, e: errors.append((m, e)),
    )

    @app.on("orders")
    def handler(message: Message) -> None:
        raise boom

    app.run()

    assert errors == [(msg, boom)]


def test_run_no_commit_on_handler_exception(mock_consumer: MagicMock) -> None:
    msg = _make_message("orders", OrderModel(order_id="x"))
    mock_consumer.consume.return_value = iter([msg])

    app = KafkianApp(mock_consumer, auto_commit=True)

    @app.on("orders")
    def handler(message: Message) -> None:
        raise RuntimeError("fail")

    app.run()

    mock_consumer.commit_offset.assert_not_called()


def test_run_loop_continues_after_handler_exception(mock_consumer: MagicMock) -> None:
    msg1 = _make_message("orders", OrderModel(order_id="a"))
    msg2 = _make_message("orders", OrderModel(order_id="b"))
    mock_consumer.consume.return_value = iter([msg1, msg2])

    committed: list[Message] = []
    app = KafkianApp(mock_consumer, auto_commit=True)

    @app.on("orders")
    def handler(message: Message) -> None:
        order = message.value
        assert isinstance(order, OrderModel)
        if order.order_id == "a":
            raise RuntimeError("first fails")
        committed.append(message)

    app.run()

    assert committed == [msg2]
    mock_consumer.commit_offset.assert_called_once_with(msg2)


def test_run_default_on_error_logs(
    mock_consumer: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    msg = _make_message("orders", OrderModel(order_id="x"))
    mock_consumer.consume.return_value = iter([msg])

    app = KafkianApp(mock_consumer)

    @app.on("orders")
    def handler(message: Message) -> None:
        raise ValueError("bad value")

    with caplog.at_level(logging.ERROR, logger="kafkian.app"):
        app.run()

    assert any("Handler raised" in r.message for r in caplog.records)

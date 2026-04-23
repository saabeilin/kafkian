from __future__ import annotations

import dataclasses
import fnmatch
import logging
from collections.abc import Callable

from kafkian.base import AvroModel, Message
from kafkian.consumer import KafkianConsumer

logger = logging.getLogger(__name__)

type HandlerFunc = Callable[[Message], None]


def _is_exact(pattern: str) -> bool:
    return not any(c in pattern for c in "*?[")


def _score_pattern(
    topic_pattern: str,
    schema_type: type[AvroModel] | None,
) -> int:
    topic_exact = _is_exact(topic_pattern)
    schema_exact = schema_type is not None
    if topic_exact and schema_exact:
        return 4
    if topic_exact:
        return 3
    if schema_exact:
        return 2
    return 1


@dataclasses.dataclass(frozen=True, slots=True)
class RouteEntry:
    topic_pattern: str
    schema_type: type[AvroModel] | None
    handler: HandlerFunc
    score: int


def _default_on_unhandled(message: Message) -> None:
    logger.warning(
        "No handler registered for topic=%s schema=%s",
        message.topic,
        type(message.value).__name__,
    )


def _default_on_error(message: Message, exc: Exception) -> None:
    logger.error(
        "Handler raised for topic=%s offset=%s",
        message.topic,
        message.offset,
        exc_info=exc,
    )


class Router:
    def __init__(self) -> None:
        self._routes: list[RouteEntry] = []

    def on(
        self,
        topic: str,
        schema_type: type[AvroModel] | None = None,
    ) -> Callable[[HandlerFunc], HandlerFunc]:
        def decorator(handler: HandlerFunc) -> HandlerFunc:
            self._routes.append(
                RouteEntry(
                    topic_pattern=topic,
                    schema_type=schema_type,
                    handler=handler,
                    score=_score_pattern(topic, schema_type),
                )
            )
            return handler

        return decorator

    def include_router(self, router: Router) -> None:
        self._routes.extend(router._routes)

    def dispatch(self, message: Message) -> HandlerFunc | None:
        best: RouteEntry | None = None
        for entry in self._routes:
            if not fnmatch.fnmatchcase(message.topic, entry.topic_pattern):
                continue
            if entry.schema_type is not None and not isinstance(
                message.value, entry.schema_type
            ):
                continue
            if best is None or entry.score > best.score:
                best = entry
        return best.handler if best is not None else None


class KafkianApp(Router):
    def __init__(
        self,
        consumer: KafkianConsumer,
        *,
        auto_commit: bool = True,
        on_unhandled: Callable[[Message], None] | None = None,
        on_error: Callable[[Message, Exception], None] | None = None,
    ) -> None:
        super().__init__()
        self.consumer = consumer
        self._auto_commit = auto_commit
        self._on_unhandled = on_unhandled or _default_on_unhandled
        self._on_error = on_error or _default_on_error

    def run(self, *, timeout: float = 1.0) -> None:
        for message in self.consumer.consume(timeout=timeout):
            self._handle(message)

    def _handle(self, message: Message) -> None:
        handler = self.dispatch(message)
        if handler is None:
            self._on_unhandled(message)
            return
        try:
            handler(message)
        except Exception as exc:
            self._on_error(message, exc)
            return
        if self._auto_commit:
            self.consumer.commit_offset(message)

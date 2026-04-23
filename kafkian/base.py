from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict


class AvroModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    _schema: ClassVar[dict[str, Any]] = {}


class Message(BaseModel):
    model_config = ConfigDict(frozen=True)

    topic: str
    value: bytes | dict[str, Any] | AvroModel | None
    key: str | bytes | None
    headers: dict[str, str | None] | None = None
    timestamp_ms: int | None = None
    partition: int | None = None
    offset: int | None = None

from __future__ import annotations

import json
from typing import Any

from confluent_kafka.schema_registry import (
    Schema,
    SchemaReference,
    SchemaRegistryClient,
)
from confluent_kafka.schema_registry.error import SchemaRegistryError

from kafkian.avsc_to_pydantic import _simple_name, find_named_deps
from kafkian.base import AvroModel

_SCHEMA_NOT_FOUND = 40403


def _default_subject(model_cls: type[AvroModel]) -> str:
    return f"{model_cls.__name__}-value"


def _resolve_references(
    schema_dict: dict[str, Any],
    client: SchemaRegistryClient,
    reference_subjects: dict[str, str] | None = None,
) -> list[SchemaReference]:
    """Build a SchemaReference list for every named-type dependency in *schema_dict*.

    Each string type reference (e.g. ``"com.example.Audit"``) is resolved to the
    latest registered version under its subject.  The default subject for a type
    reference is ``"{SimpleName}-value"``; supply *reference_subjects* to override
    individual mappings.

    All referenced schemas must already be registered — this function will raise
    ``SchemaRegistryError`` if a dependency cannot be found.
    """
    deps = find_named_deps(schema_dict)
    if not deps:
        return []

    overrides = reference_subjects or {}
    references: list[SchemaReference] = []
    for dep in sorted(deps):  # sorted for deterministic ordering
        subject = overrides.get(dep) or f"{_simple_name(dep)}-value"
        version = client.get_latest_version(subject).version
        references.append(SchemaReference(dep, subject, version))
    return references


class SchemaRegistry:
    """Model-aware facade over SchemaRegistryClient.

    Handles ``SchemaReference`` wiring automatically: any named-type string
    reference found in a model's ``_schema`` (e.g. ``"com.example.Audit"``) is
    resolved to the latest registered version and embedded as a
    ``SchemaReference`` when building or registering a schema.

    Call ``ensure_registered`` before producing — or let ``KafkianProducer``
    call it lazily on first produce.  Dependencies are registered recursively
    in topological order; re-registration is skipped via an in-memory cache.

    Subject defaults to ``{ModelClassName}-value``; pass an explicit *subject* or
    *reference_subjects* override when a different naming convention is in use.

    Usage::

        sr = SchemaRegistry(SchemaRegistryClient({"url": "http://localhost:8081"}))
        sr.register_model(AuditModel)
        sr.register_model(OrderCreatedModel)

        producer = KafkianProducer(
            Producer({"bootstrap.servers": "localhost:9092"}),
            sr,
        )
    """

    def __init__(self, client: SchemaRegistryClient) -> None:
        self._client = client
        self._model_registry: dict[str, type[AvroModel]] = {}
        self._registered: dict[str, int] = {}  # subject → schema_id

    # ------------------------------------------------------------------
    # Model index
    # ------------------------------------------------------------------

    def register_model(self, model_cls: type[AvroModel]) -> None:
        """Index *model_cls* so its schema can be auto-resolved as a dependency.

        Call this for every model class before producing, or pass them explicitly
        to ``ensure_registered``.  Idempotent.
        """
        schema_dict: dict[str, Any] = model_cls._schema
        name: str = schema_dict.get("name", model_cls.__name__)
        namespace: str = schema_dict.get("namespace", "")
        self._model_registry[name] = model_cls
        self._model_registry[_simple_name(name)] = model_cls
        if namespace:
            self._model_registry[f"{namespace}.{name}"] = model_cls
            self._model_registry[f"{namespace}.{_simple_name(name)}"] = model_cls

    def lookup_model(self, name: str) -> type[AvroModel] | None:
        """Return the model class indexed under *name*, or ``None`` if not found.

        *name* may be a simple class name, a fully-qualified ``namespace.Name``,
        or any variant stored by :meth:`register_model`.
        """
        return self._model_registry.get(name)

    # ------------------------------------------------------------------
    # Schema construction
    # ------------------------------------------------------------------

    def build_schema(
        self,
        model_cls: type[AvroModel],
        reference_subjects: dict[str, str] | None = None,
    ) -> Schema:
        """Return a ``Schema`` instance for *model_cls* with ``SchemaReference``
        objects for every cross-schema dependency.

        Referenced schemas must already be registered so their versions can be
        pinned.  Pass *reference_subjects* to override the default subject name
        (``{SimpleName}-value``) for individual type references.
        """
        refs = _resolve_references(model_cls._schema, self._client, reference_subjects)
        return Schema(json.dumps(model_cls._schema), "AVRO", refs)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        model_cls: type[AvroModel],
        subject: str | None = None,
        reference_subjects: dict[str, str] | None = None,
    ) -> int:
        """Register the model's schema unconditionally.

        Returns the schema ID assigned by Schema Registry.  Raises if the
        schema is incompatible with an already-registered version under the
        same subject.
        """
        return self._client.register_schema(
            subject or _default_subject(model_cls),
            self.build_schema(model_cls, reference_subjects),
        )

    def ensure_registered(
        self,
        model_cls: type[AvroModel],
        subject: str | None = None,
        reference_subjects: dict[str, str] | None = None,
    ) -> int:
        """Idempotently register the model's schema.

        Indexes *model_cls* in the model registry, then recursively ensures
        all named-type dependencies are registered first (using any previously
        indexed models).  Results are cached in-memory so re-registration is
        skipped within the same ``SchemaRegistry`` instance.

        Returns the schema ID whether this call registered it or it was
        already present.
        """
        resolved = subject or _default_subject(model_cls)
        if resolved in self._registered:
            return self._registered[resolved]

        self.register_model(model_cls)

        for dep_name in sorted(find_named_deps(model_cls._schema)):
            dep_cls = self._model_registry.get(dep_name)
            if dep_cls is not None and dep_cls is not model_cls:
                self.ensure_registered(dep_cls)

        schema = self.build_schema(model_cls, reference_subjects)
        try:
            schema_id = self._client.lookup_schema(resolved, schema).schema_id
        except SchemaRegistryError as exc:
            if exc.error_code == _SCHEMA_NOT_FOUND:
                schema_id = self._client.register_schema(resolved, schema)
            else:
                raise
        self._registered[resolved] = schema_id
        return schema_id

    def is_registered(
        self,
        model_cls: type[AvroModel],
        subject: str | None = None,
        reference_subjects: dict[str, str] | None = None,
    ) -> bool:
        """Return ``True`` if the exact schema (including its references) is
        already registered under *subject*.

        Note: resolves referenced schema versions from Schema Registry, so all
        dependency schemas must already be registered.
        """
        resolved = subject or _default_subject(model_cls)
        schema = self.build_schema(model_cls, reference_subjects)
        try:
            self._client.lookup_schema(resolved, schema)
            return True
        except SchemaRegistryError as exc:
            if exc.error_code == _SCHEMA_NOT_FOUND:
                return False
            raise

    @property
    def client(self) -> SchemaRegistryClient:
        """The underlying SchemaRegistryClient for operations not covered here."""
        return self._client

from __future__ import annotations

import json
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError

from kafkian.base import AvroModel
from kafkian.schema_registry import SchemaRegistry


class OrderModel(AvroModel):
    _schema: ClassVar[dict[str, Any]] = {
        "type": "record",
        "name": "OrderModel",
        "fields": [{"name": "id", "type": "string"}],
    }
    id: str


# Schema that references an external named type
class OrderWithAuditModel(AvroModel):
    _schema: ClassVar[dict[str, Any]] = {
        "type": "record",
        "name": "OrderWithAuditModel",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "audit", "type": "com.example.Audit"},
        ],
    }
    id: str
    audit: str  # simplified for unit testing purposes


def _sr_error(error_code: int) -> SchemaRegistryError:
    return SchemaRegistryError(422, error_code, "test error")


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def sr(mock_client: MagicMock) -> SchemaRegistry:
    return SchemaRegistry(mock_client)


# ---------------------------------------------------------------------------
# default subject naming
# ---------------------------------------------------------------------------


def test_register_uses_default_subject(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.register_schema.return_value = 1
    sr.register(OrderModel)
    subject = mock_client.register_schema.call_args.args[0]
    assert subject == "OrderModel-value"


def test_register_uses_explicit_subject(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.register_schema.return_value = 1
    sr.register(OrderModel, subject="orders-value")
    subject = mock_client.register_schema.call_args.args[0]
    assert subject == "orders-value"


# ---------------------------------------------------------------------------
# register()
# ---------------------------------------------------------------------------


def test_register_returns_schema_id(sr: SchemaRegistry, mock_client: MagicMock) -> None:
    mock_client.register_schema.return_value = 42
    assert sr.register(OrderModel) == 42


def test_register_passes_avro_schema(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.register_schema.return_value = 1
    sr.register(OrderModel)
    schema_arg = mock_client.register_schema.call_args.args[1]
    assert schema_arg.schema_str == json.dumps(OrderModel._schema)
    assert schema_arg.schema_type == "AVRO"


def test_register_propagates_incompatibility_error(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.register_schema.side_effect = _sr_error(409)
    with pytest.raises(SchemaRegistryError):
        sr.register(OrderModel)


# ---------------------------------------------------------------------------
# ensure_registered()
# ---------------------------------------------------------------------------


def test_ensure_registered_returns_existing_id_when_already_present(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    existing = MagicMock()
    existing.schema_id = 7
    mock_client.lookup_schema.return_value = existing

    result = sr.ensure_registered(OrderModel)

    assert result == 7
    mock_client.register_schema.assert_not_called()


def test_ensure_registered_registers_when_not_found(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.lookup_schema.side_effect = _sr_error(40403)
    mock_client.register_schema.return_value = 99

    result = sr.ensure_registered(OrderModel)

    assert result == 99
    mock_client.register_schema.assert_called_once()


def test_ensure_registered_propagates_unexpected_sr_error(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.lookup_schema.side_effect = _sr_error(500)
    with pytest.raises(SchemaRegistryError):
        sr.ensure_registered(OrderModel)


def test_ensure_registered_passes_same_schema_to_both_calls(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.lookup_schema.side_effect = _sr_error(40403)
    mock_client.register_schema.return_value = 1

    sr.ensure_registered(OrderModel)

    lookup_schema = mock_client.lookup_schema.call_args.args[1]
    register_schema = mock_client.register_schema.call_args.args[1]
    assert (
        lookup_schema.schema_str
        == register_schema.schema_str
        == json.dumps(OrderModel._schema)
    )


# ---------------------------------------------------------------------------
# is_registered()
# ---------------------------------------------------------------------------


def test_is_registered_true_when_found(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.lookup_schema.return_value = MagicMock()
    assert sr.is_registered(OrderModel) is True


def test_is_registered_false_when_not_found(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.lookup_schema.side_effect = _sr_error(40403)
    assert sr.is_registered(OrderModel) is False


def test_is_registered_propagates_unexpected_error(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.lookup_schema.side_effect = _sr_error(401)
    with pytest.raises(SchemaRegistryError):
        sr.is_registered(OrderModel)


def test_is_registered_uses_explicit_subject(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.lookup_schema.return_value = MagicMock()
    sr.is_registered(OrderModel, subject="custom-subject")
    assert mock_client.lookup_schema.call_args.args[0] == "custom-subject"


# ---------------------------------------------------------------------------
# client property
# ---------------------------------------------------------------------------


def test_client_property_exposes_underlying_client(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    assert sr.client is mock_client


# ---------------------------------------------------------------------------
# build_schema() — cross-schema references
# ---------------------------------------------------------------------------


def _mock_version(n: int) -> MagicMock:
    v = MagicMock()
    v.version = n
    return v


def test_build_schema_no_refs_does_not_call_get_latest_version(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    schema = sr.build_schema(OrderModel)
    mock_client.get_latest_version.assert_not_called()
    assert schema.schema_str == json.dumps(OrderModel._schema)
    assert schema.schema_type == "AVRO"
    assert schema.references == []


def test_build_schema_with_refs_fetches_dependency_version(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.get_latest_version.return_value = _mock_version(3)

    schema = sr.build_schema(OrderWithAuditModel)

    mock_client.get_latest_version.assert_called_once_with("Audit-value")
    assert schema.references is not None
    assert len(schema.references) == 1
    ref = schema.references[0]
    assert ref.name == "com.example.Audit"
    assert ref.subject == "Audit-value"
    assert ref.version == 3


def test_build_schema_reference_subject_override(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.get_latest_version.return_value = _mock_version(1)

    sr.build_schema(
        OrderWithAuditModel,
        reference_subjects={"com.example.Audit": "audit-events-value"},
    )

    mock_client.get_latest_version.assert_called_once_with("audit-events-value")


def test_build_schema_propagates_sr_error_for_missing_dependency(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.get_latest_version.side_effect = _sr_error(40401)
    with pytest.raises(SchemaRegistryError):
        sr.build_schema(OrderWithAuditModel)


# ---------------------------------------------------------------------------
# register() / ensure_registered() pass SchemaReference to SR client
# ---------------------------------------------------------------------------


def test_register_with_refs_passes_schema_reference(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.get_latest_version.return_value = _mock_version(2)
    mock_client.register_schema.return_value = 10

    sr.register(OrderWithAuditModel)

    schema_arg = mock_client.register_schema.call_args.args[1]
    assert len(schema_arg.references) == 1
    assert schema_arg.references[0].name == "com.example.Audit"
    assert schema_arg.references[0].version == 2


def test_ensure_registered_with_refs_registers_with_schema_reference(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    mock_client.get_latest_version.return_value = _mock_version(5)
    mock_client.lookup_schema.side_effect = _sr_error(40403)
    mock_client.register_schema.return_value = 20

    result = sr.ensure_registered(OrderWithAuditModel)

    assert result == 20
    schema_arg = mock_client.register_schema.call_args.args[1]
    assert schema_arg.references[0].version == 5


# ---------------------------------------------------------------------------
# register_model() / _model_registry indexing
# ---------------------------------------------------------------------------


class AuditModel(AvroModel):
    _schema: ClassVar[dict[str, Any]] = {
        "type": "record",
        "name": "Audit",
        "namespace": "com.example",
        "fields": [{"name": "created_by", "type": "string"}],
    }
    created_by: str


class OrderWithAutoAuditModel(AvroModel):
    _schema: ClassVar[dict[str, Any]] = {
        "type": "record",
        "name": "OrderWithAutoAuditModel",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "audit", "type": "com.example.Audit"},
        ],
    }
    id: str
    audit: str


def test_register_model_indexes_by_simple_name(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    sr.register_model(AuditModel)
    assert sr._model_registry["Audit"] is AuditModel


def test_register_model_indexes_by_full_name(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    sr.register_model(AuditModel)
    assert sr._model_registry["com.example.Audit"] is AuditModel


# ---------------------------------------------------------------------------
# ensure_registered() — dep traversal and cache
# ---------------------------------------------------------------------------


def test_ensure_registered_uses_cache_on_second_call(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    existing = MagicMock()
    existing.schema_id = 7
    mock_client.lookup_schema.return_value = existing

    sr.ensure_registered(OrderModel)
    sr.ensure_registered(OrderModel)

    assert mock_client.lookup_schema.call_count == 1


def test_ensure_registered_returns_cached_id(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    existing = MagicMock()
    existing.schema_id = 42
    mock_client.lookup_schema.return_value = existing

    first = sr.ensure_registered(OrderModel)
    second = sr.ensure_registered(OrderModel)

    assert first == second == 42


def test_ensure_registered_auto_registers_known_dep(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    """When a dep model is indexed, ensure_registered calls itself recursively for it."""
    mock_client.get_latest_version.return_value = _mock_version(1)
    mock_client.lookup_schema.side_effect = _sr_error(40403)
    mock_client.register_schema.side_effect = [5, 10]  # audit first, then order

    sr.register_model(AuditModel)
    result = sr.ensure_registered(OrderWithAutoAuditModel)

    assert result == 10
    # Both subjects registered
    subjects = [c.args[0] for c in mock_client.register_schema.call_args_list]
    assert "AuditModel-value" in subjects
    assert "OrderWithAutoAuditModel-value" in subjects
    # Audit registered before the referencing schema
    assert subjects.index("AuditModel-value") < subjects.index(
        "OrderWithAutoAuditModel-value"
    )


def test_ensure_registered_dep_not_registered_twice(
    sr: SchemaRegistry, mock_client: MagicMock
) -> None:
    """Once a dep is registered, it is not re-registered when another model needs it."""
    mock_client.get_latest_version.return_value = _mock_version(1)
    mock_client.lookup_schema.side_effect = _sr_error(40403)
    mock_client.register_schema.side_effect = [5, 10, 20]

    sr.register_model(AuditModel)
    sr.ensure_registered(AuditModel)  # registers Audit (id=5)
    sr.ensure_registered(
        OrderWithAutoAuditModel
    )  # registers Order (id=10), skips Audit

    audit_calls = [
        c
        for c in mock_client.register_schema.call_args_list
        if c.args[0] == "AuditModel-value"
    ]
    assert len(audit_calls) == 1

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kafkian.avsc_to_pydantic import (
    collect_named_types,
    generate_from_dir,
    python_literal,
    type_annotation,
)

# ---------------------------------------------------------------------------
# type_annotation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("avro_type", "expected"),
    [
        ("string", "str"),
        ("int", "int"),
        ("long", "int"),
        ("float", "float"),
        ("double", "float"),
        ("boolean", "bool"),
        ("bytes", "bytes"),
        ("null", "None"),
        # Named reference (namespaced)
        ("com.example.Address", "Address"),
        # Union: nullable
        (["null", "string"], "str | None"),
        (["string", "null"], "str | None"),
        # Union: all-null (edge case)
        (["null"], "None"),
        # Union: non-nullable multi-type
        (["string", "int"], "str | int"),
        # Array
        ({"type": "array", "items": "string"}, "list[str]"),
        # Map
        ({"type": "map", "values": "int"}, "dict[str, int]"),
        # Enum reference via inline definition
        ({"type": "enum", "name": "Status", "symbols": ["A", "B"]}, "Status"),
        # Fixed
        ({"type": "fixed", "name": "Md5", "size": 16}, "bytes"),
        # Logical types
        ({"type": "int", "logicalType": "date"}, "datetime.date"),
        ({"type": "long", "logicalType": "timestamp-millis"}, "datetime.datetime"),
        ({"type": "string", "logicalType": "uuid"}, "uuid.UUID"),
        (
            {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2},
            "decimal.Decimal",
        ),
    ],
)
def test_type_annotation(avro_type: object, expected: str) -> None:
    imports: set[str] = set()
    assert type_annotation(avro_type, imports) == expected  # type: ignore[arg-type]


def test_type_annotation_collects_stdlib_imports() -> None:
    imports: set[str] = set()
    type_annotation({"type": "int", "logicalType": "date"}, imports)
    assert "datetime" in imports

    imports2: set[str] = set()
    type_annotation({"type": "string", "logicalType": "uuid"}, imports2)
    assert "uuid" in imports2


def test_type_annotation_unknown_type_does_not_pollute_imports() -> None:
    imports: set[str] = set()
    result = type_annotation({"type": "unknown_custom"}, imports)
    assert result == "Any"
    assert "Any" not in imports


# ---------------------------------------------------------------------------
# python_literal
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "None"),
        (True, "True"),
        (False, "False"),
        (42, "42"),
        (3.14, "3.14"),
        ("hello", "'hello'"),
        ([], "[]"),
        ({}, "{}"),
    ],
)
def test_python_literal(value: object, expected: str) -> None:
    assert python_literal(value) == expected


# ---------------------------------------------------------------------------
# collect_named_types
# ---------------------------------------------------------------------------


def test_collect_named_types_simple_record() -> None:
    schema = {
        "type": "record",
        "name": "User",
        "fields": [{"name": "id", "type": "string"}],
    }
    result: list[dict] = []
    collect_named_types(schema, result)
    assert len(result) == 1
    assert result[0]["name"] == "User"


def test_collect_named_types_with_enum() -> None:
    schema = {
        "type": "record",
        "name": "Event",
        "fields": [
            {
                "name": "status",
                "type": {"type": "enum", "name": "Status", "symbols": ["OK", "ERR"]},
            },
        ],
    }
    result: list[dict] = []
    collect_named_types(schema, result)
    # Enum is collected before the record (DFS order)
    assert result[0]["name"] == "Status"
    assert result[1]["name"] == "Event"


def test_collect_named_types_nested_records() -> None:
    schema = {
        "type": "record",
        "name": "Order",
        "fields": [
            {
                "name": "address",
                "type": {
                    "type": "record",
                    "name": "Address",
                    "fields": [{"name": "city", "type": "string"}],
                },
            },
        ],
    }
    result: list[dict] = []
    collect_named_types(schema, result)
    assert result[0]["name"] == "Address"
    assert result[1]["name"] == "Order"


# ---------------------------------------------------------------------------
# generate_from_dir — integration tests using tmp_path
# ---------------------------------------------------------------------------


@pytest.fixture
def avsc_dir(tmp_path: Path) -> Path:
    return tmp_path


def write_avsc(directory: Path, name: str, schema: dict) -> None:
    (directory / f"{name}.avsc").write_text(json.dumps(schema))


def test_generate_empty_dir(avsc_dir: Path) -> None:
    assert generate_from_dir(avsc_dir) == "# No .avsc files found\n"


def test_generate_simple_record(avsc_dir: Path) -> None:
    write_avsc(
        avsc_dir,
        "user",
        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "age", "type": ["null", "int"], "default": None},
            ],
        },
    )
    source = generate_from_dir(avsc_dir)

    assert "class User(AvroModel):" in source
    assert "_schema: ClassVar[dict[str, Any]]" in source
    assert "id: str" in source
    assert "age: int | None = None" in source
    # Required field comes before optional
    assert source.index("id: str") < source.index("age: int | None")


def test_generate_with_enum(avsc_dir: Path) -> None:
    write_avsc(
        avsc_dir,
        "event",
        {
            "type": "record",
            "name": "Event",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["ACTIVE", "INACTIVE"],
                    },
                },
            ],
        },
    )
    source = generate_from_dir(avsc_dir)

    assert "class Status(str, enum.Enum):" in source
    assert 'ACTIVE = "ACTIVE"' in source
    assert "class Event(AvroModel):" in source
    # Enum class must be defined before the record that uses it
    assert source.index("class Status") < source.index("class Event")


def test_generate_logical_types(avsc_dir: Path) -> None:
    write_avsc(
        avsc_dir,
        "timestamped",
        {
            "type": "record",
            "name": "Timestamped",
            "fields": [
                {
                    "name": "created_at",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {"name": "event_date", "type": {"type": "int", "logicalType": "date"}},
            ],
        },
    )
    source = generate_from_dir(avsc_dir)

    assert "import datetime" in source
    assert "created_at: datetime.datetime" in source
    assert "event_date: datetime.date" in source


def test_generate_schema_dict_has_correct_name(avsc_dir: Path) -> None:
    import ast

    schema = {
        "type": "record",
        "name": "Item",
        "fields": [{"name": "name", "type": "string"}],
    }
    write_avsc(avsc_dir, "item", schema)
    source = generate_from_dir(avsc_dir)

    tree = ast.parse(source)
    schema_values = [
        ast.literal_eval(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "_schema"
        and node.value is not None
    ]
    assert len(schema_values) == 1
    assert schema_values[0]["name"] == "Item"


def test_generate_multiple_avsc_files(avsc_dir: Path) -> None:
    write_avsc(
        avsc_dir,
        "address",
        {
            "type": "record",
            "name": "Address",
            "fields": [{"name": "city", "type": "string"}],
        },
    )
    write_avsc(
        avsc_dir,
        "user",
        {
            "type": "record",
            "name": "User",
            "fields": [{"name": "name", "type": "string"}],
        },
    )
    source = generate_from_dir(avsc_dir)

    assert "class Address(AvroModel):" in source
    assert "class User(AvroModel):" in source


def test_generate_inherits_avro_model(avsc_dir: Path) -> None:
    write_avsc(avsc_dir, "thing", {"type": "record", "name": "Thing", "fields": []})
    source = generate_from_dir(avsc_dir)
    assert "from kafkian.base import AvroModel" in source
    assert "class Thing(AvroModel):" in source


# ---------------------------------------------------------------------------
# Cross-file schema references — the shared-audit scenario
# ---------------------------------------------------------------------------

_AUDIT_SCHEMA = {
    "type": "record",
    "name": "Audit",
    "namespace": "com.example",
    "fields": [
        {"name": "created_by", "type": "string"},
        {
            "name": "created_at",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
    ],
}

_ORDER_SCHEMA = {
    "type": "record",
    "name": "OrderCreated",
    "namespace": "com.example",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "audit", "type": "com.example.Audit"},
    ],
}

_PAYMENT_SCHEMA = {
    "type": "record",
    "name": "PaymentProcessed",
    "namespace": "com.example",
    "fields": [
        {"name": "payment_id", "type": "string"},
        {"name": "audit", "type": "com.example.Audit"},
    ],
}


@pytest.fixture
def cross_ref_dir(avsc_dir: Path) -> Path:
    write_avsc(avsc_dir, "audit", _AUDIT_SCHEMA)
    write_avsc(avsc_dir, "order", _ORDER_SCHEMA)
    write_avsc(avsc_dir, "payment", _PAYMENT_SCHEMA)
    return avsc_dir


def test_cross_ref_generates_three_models(cross_ref_dir: Path) -> None:
    source = generate_from_dir(cross_ref_dir)
    assert source.count("class") == 3  # Audit, OrderCreated, PaymentProcessed


def test_cross_ref_all_class_names_present(cross_ref_dir: Path) -> None:
    source = generate_from_dir(cross_ref_dir)
    assert "class Audit(AvroModel):" in source
    assert "class OrderCreated(AvroModel):" in source
    assert "class PaymentProcessed(AvroModel):" in source


def test_cross_ref_audit_defined_before_dependants(cross_ref_dir: Path) -> None:
    source = generate_from_dir(cross_ref_dir)
    audit_pos = source.index("class Audit(AvroModel):")
    assert audit_pos < source.index("class OrderCreated(AvroModel):")
    assert audit_pos < source.index("class PaymentProcessed(AvroModel):")


def test_cross_ref_audit_defined_before_dependants_reversed_file_order(
    avsc_dir: Path,
) -> None:
    # Write in reverse alphabetical order so file-sort would put business events first
    write_avsc(avsc_dir, "payment", _PAYMENT_SCHEMA)
    write_avsc(avsc_dir, "order", _ORDER_SCHEMA)
    write_avsc(avsc_dir, "audit", _AUDIT_SCHEMA)
    source = generate_from_dir(avsc_dir)
    audit_pos = source.index("class Audit(AvroModel):")
    assert audit_pos < source.index("class OrderCreated(AvroModel):")
    assert audit_pos < source.index("class PaymentProcessed(AvroModel):")


def test_cross_ref_field_type_annotation(cross_ref_dir: Path) -> None:
    source = generate_from_dir(cross_ref_dir)
    assert "audit: Audit" in source


def test_cross_ref_each_model_has_own_schema(cross_ref_dir: Path) -> None:
    source = generate_from_dir(cross_ref_dir)
    assert source.count("_schema: ClassVar[dict[str, Any]]") == 3


def test_cross_ref_schemas_contain_correct_record_names(cross_ref_dir: Path) -> None:
    import ast

    source = generate_from_dir(cross_ref_dir)
    tree = ast.parse(source)
    names = {
        ast.literal_eval(node.value)["name"]
        for node in ast.walk(tree)
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "_schema"
        and node.value is not None
    }
    assert names == {"Audit", "OrderCreated", "PaymentProcessed"}


# ---------------------------------------------------------------------------
# Namespace-collision dedup — same simple name in different namespaces
# ---------------------------------------------------------------------------


def test_same_name_different_namespace_both_generated(avsc_dir: Path) -> None:
    """Two schemas sharing a simple name in different namespaces get distinct class names."""
    write_avsc(
        avsc_dir,
        "event_a",
        {
            "type": "record",
            "name": "Event",
            "namespace": "com.a",
            "fields": [{"name": "x", "type": "string"}],
        },
    )
    write_avsc(
        avsc_dir,
        "event_b",
        {
            "type": "record",
            "name": "Event",
            "namespace": "com.b",
            "fields": [{"name": "y", "type": "int"}],
        },
    )
    source = generate_from_dir(avsc_dir)
    # Each schema gets a namespace-prefixed class name to avoid shadowing
    assert "class AEvent(AvroModel):" in source
    assert "class BEvent(AvroModel):" in source
    assert source.count("class AEvent") == 1
    assert source.count("class BEvent") == 1
    # No ambiguous unqualified class is emitted
    assert "class Event(AvroModel):" not in source


def test_same_name_different_namespace_field_ref_resolved(avsc_dir: Path) -> None:
    """Field annotations referencing a disambiguated type use the new Python class name."""
    write_avsc(
        avsc_dir,
        "event_a",
        {
            "type": "record",
            "name": "Event",
            "namespace": "com.a",
            "fields": [{"name": "x", "type": "string"}],
        },
    )
    write_avsc(
        avsc_dir,
        "event_b",
        {
            "type": "record",
            "name": "Event",
            "namespace": "com.b",
            "fields": [{"name": "y", "type": "int"}],
        },
    )
    write_avsc(
        avsc_dir,
        "container",
        {
            "type": "record",
            "name": "Container",
            "fields": [{"name": "evt", "type": "com.a.Event"}],
        },
    )
    source = generate_from_dir(avsc_dir)
    assert "evt: AEvent" in source

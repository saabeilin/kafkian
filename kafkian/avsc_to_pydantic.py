from __future__ import annotations

import json
import pprint
from pathlib import Path
from typing import Any

type AvroType = str | dict[str, Any] | list[Any]

_PRIMITIVES: dict[str, str] = {
    "null": "None",
    "boolean": "bool",
    "int": "int",
    "long": "int",
    "float": "float",
    "double": "float",
    "bytes": "bytes",
    "string": "str",
}

# (annotation_string, stdlib_module_to_import)
_LOGICAL: dict[str, tuple[str, str]] = {
    "date": ("datetime.date", "datetime"),
    "time-millis": ("datetime.time", "datetime"),
    "time-micros": ("datetime.time", "datetime"),
    "timestamp-millis": ("datetime.datetime", "datetime"),
    "timestamp-micros": ("datetime.datetime", "datetime"),
    "local-timestamp-millis": ("datetime.datetime", "datetime"),
    "local-timestamp-micros": ("datetime.datetime", "datetime"),
    "uuid": ("uuid.UUID", "uuid"),
    "decimal": ("decimal.Decimal", "decimal"),
}


def _simple_name(full_name: str) -> str:
    return full_name.split(".")[-1]


def type_annotation(avro_type: AvroType, imports: set[str]) -> str:
    """Convert an Avro type definition to a Python type annotation string.

    Modifies *imports* in-place with any stdlib modules required.
    """
    if isinstance(avro_type, str):
        return _PRIMITIVES.get(avro_type, _simple_name(avro_type))

    if isinstance(avro_type, list):
        parts = [type_annotation(t, imports) for t in avro_type]
        non_none = [p for p in parts if p != "None"]
        has_none = len(non_none) < len(parts)
        joined = " | ".join(non_none)
        return f"{joined} | None" if has_none else joined

    logical = avro_type.get("logicalType")
    if logical in _LOGICAL:
        ann, mod = _LOGICAL[logical]
        imports.add(mod)
        return ann

    kind = avro_type.get("type")

    match kind:
        case "record":
            return _simple_name(avro_type["name"])
        case "array":
            item = type_annotation(avro_type["items"], imports)
            return f"list[{item}]"
        case "map":
            val = type_annotation(avro_type["values"], imports)
            return f"dict[str, {val}]"
        case "enum":
            return _simple_name(avro_type["name"])
        case "fixed":
            return "bytes"
        case _ if kind in _PRIMITIVES:
            return _PRIMITIVES[kind]
        case _:
            imports.add("Any")
            return "Any"


def python_literal(value: Any) -> str:
    """Convert an Avro field default value to a Python literal string."""
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "True" if value else "False"
    return repr(value)


def collect_named_types(schema: AvroType, result: list[dict[str, Any]]) -> None:
    """Depth-first collect all record and enum definitions in dependency order."""
    if isinstance(schema, list):
        for item in schema:
            collect_named_types(item, result)
        return
    if not isinstance(schema, dict):
        return

    match schema.get("type"):
        case "record":
            for field in schema.get("fields", []):
                collect_named_types(field.get("type"), result)
            result.append(schema)
        case "enum":
            result.append(schema)
        case "array":
            collect_named_types(schema.get("items"), result)
        case "map":
            collect_named_types(schema.get("values"), result)


def find_named_deps(avro_type: AvroType) -> set[str]:
    """Return every named-type string reference found anywhere inside *avro_type*.

    These are the cross-schema dependencies used for topological sorting.
    Primitive type names are excluded.
    """
    deps: set[str] = set()

    def walk(node: AvroType) -> None:
        if isinstance(node, str):
            if node not in _PRIMITIVES:
                deps.add(node)
        elif isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            match node.get("type"):
                case "record":
                    for field in node.get("fields", []):
                        walk(field.get("type"))
                case "array":
                    walk(node.get("items"))
                case "map":
                    walk(node.get("values"))

    walk(avro_type)
    return deps


def _build_name_map(
    named_types: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Index named types by every alias they can be referenced as.

    A schema with ``name="Audit"`` and ``namespace="com.example"`` is reachable
    as both ``"Audit"`` and ``"com.example.Audit"``.
    """
    index: dict[str, dict[str, Any]] = {}
    for schema in named_types:
        name: str = schema["name"]
        namespace: str = schema.get("namespace", "")
        index[name] = schema
        index[_simple_name(name)] = schema
        if namespace:
            index[f"{namespace}.{name}"] = schema
            index[f"{namespace}.{_simple_name(name)}"] = schema
    return index


def _topo_sort(
    named_types: list[dict[str, Any]],
    name_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return *named_types* ordered so every dependency precedes its dependant.

    Uses iterative DFS to avoid recursion limits on deep schema chains.
    Schemas whose dependencies are not in *name_map* (external references) are
    silently ignored for ordering purposes.
    """
    result: list[dict[str, Any]] = []
    visited: set[str] = set()

    def visit(schema: dict[str, Any]) -> None:
        name = schema["name"]
        if name in visited:
            return
        visited.add(name)
        for dep_ref in find_named_deps(schema):
            dep = name_map.get(dep_ref)
            if dep is not None and dep["name"] != name:
                visit(dep)
        result.append(schema)

    for schema in named_types:
        visit(schema)

    return result


def _render_enum(schema: dict[str, Any]) -> str:
    name = _simple_name(schema["name"])
    lines = [f"class {name}(str, enum.Enum):"]
    for sym in schema["symbols"]:
        lines.append(f'    {sym} = "{sym}"')
    return "\n".join(lines)


def _render_record(schema: dict[str, Any], avsc_dict: dict[str, Any]) -> str:
    dummy: set[str] = set()
    fields = schema.get("fields", [])

    # Required fields must precede fields with defaults
    required = [f for f in fields if "default" not in f]
    optional = [f for f in fields if "default" in f]

    dict_repr = pprint.pformat(avsc_dict, sort_dicts=False)
    indented_repr = dict_repr.replace("\n", "\n    ")
    lines = [
        f"class {_simple_name(schema['name'])}(AvroModel):",
        f"    _schema: ClassVar[dict[str, Any]] = {indented_repr}",
    ]

    field_lines = []
    for field in required + optional:
        ann = type_annotation(field["type"], dummy)
        if "default" in field:
            field_lines.append(
                f"    {field['name']}: {ann} = {python_literal(field['default'])}"
            )
        else:
            field_lines.append(f"    {field['name']}: {ann}")

    if field_lines:
        lines.append("")
        lines.extend(field_lines)

    return "\n".join(lines)


def load_avsc(path: Path) -> list[dict[str, Any]]:
    """Parse an .avsc file; a file may contain a single schema or a JSON array of schemas."""
    raw = json.loads(path.read_text())
    return raw if isinstance(raw, list) else [raw]


def generate_from_dir(avsc_dir: Path) -> str:
    """Return a Python module source with Pydantic models for all .avsc files in *avsc_dir*."""
    schemas: list[dict[str, Any]] = []
    for path in sorted(avsc_dir.glob("*.avsc")):
        schemas.extend(load_avsc(path))

    if not schemas:
        return "# No .avsc files found\n"

    # Collect named types from every file, deduplicate by full name (keep first)
    named_types: list[dict[str, Any]] = []
    seen: set[str] = set()
    for schema in schemas:
        candidates: list[dict[str, Any]] = []
        collect_named_types(schema, candidates)
        for named in candidates:
            if named["name"] not in seen:
                seen.add(named["name"])
                named_types.append(named)

    # Topologically sort so cross-file dependencies always precede their dependants
    name_map = _build_name_map(named_types)
    named_types = _topo_sort(named_types, name_map)

    # Determine which stdlib modules are needed
    stdlib_imports: set[str] = set()
    for named in named_types:
        if named.get("type") == "record":
            for field in named.get("fields", []):
                type_annotation(field["type"], stdlib_imports)

    has_enum = any(n.get("type") == "enum" for n in named_types)

    # Map each top-level schema by name so nested records point to their full parent
    top_level_by_name: dict[str, dict[str, Any]] = {s["name"]: s for s in schemas}

    import_lines = ["from __future__ import annotations"]
    for mod in sorted(stdlib_imports):
        import_lines.append(f"import {mod}")
    if has_enum:
        import_lines.append("import enum")
    import_lines.append("from typing import Any, ClassVar")
    import_lines.append("from kafkian.base import AvroModel")

    blocks: list[str] = []
    for named in named_types:
        match named.get("type"):
            case "enum":
                blocks.append(_render_enum(named))
            case "record":
                avsc_dict = top_level_by_name.get(named["name"], named)
                blocks.append(_render_record(named, avsc_dict))

    return "\n".join(import_lines) + "\n\n\n" + "\n\n\n".join(blocks) + "\n"

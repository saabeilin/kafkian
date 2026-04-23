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


def _union_annotation(parts: list[str]) -> str:
    """Build a Python union annotation from resolved member annotations."""
    non_none = [p for p in parts if p != "None"]
    if not non_none:
        return "None"
    joined = " | ".join(non_none)
    return f"{joined} | None" if len(non_none) < len(parts) else joined


def _kind_annotation(
    kind: str | None, avro_type: dict[str, Any], imports: set[str]
) -> str:
    match kind:
        case "record" | "enum":
            return _simple_name(avro_type["name"])
        case "array":
            return f"list[{type_annotation(avro_type['items'], imports)}]"
        case "map":
            return f"dict[str, {type_annotation(avro_type['values'], imports)}]"
        case "fixed":
            return "bytes"
        case _ if kind in _PRIMITIVES:
            return _PRIMITIVES[kind]
        case _:
            return "Any"


def type_annotation(avro_type: AvroType, imports: set[str]) -> str:
    """Convert an Avro type definition to a Python type annotation string.

    Modifies *imports* in-place with any stdlib modules required.
    """
    if isinstance(avro_type, str):
        return _PRIMITIVES.get(avro_type, _simple_name(avro_type))
    if isinstance(avro_type, list):
        return _union_annotation([type_annotation(t, imports) for t in avro_type])
    logical = avro_type.get("logicalType")
    if logical in _LOGICAL:
        ann, mod = _LOGICAL[logical]
        imports.add(mod)
        return ann
    return _kind_annotation(avro_type.get("type"), avro_type, imports)


def python_literal(value: Any) -> str:
    """Convert an Avro field default value to a Python literal string."""
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "True" if value else "False"
    return repr(value)


def _collect_record(schema: dict[str, Any], result: list[dict[str, Any]]) -> None:
    for field in schema.get("fields", []):
        collect_named_types(field.get("type", "null"), result)
    result.append(schema)


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
            _collect_record(schema, result)
        case "enum":
            result.append(schema)
        case "array":
            collect_named_types(schema.get("items", "null"), result)
        case "map":
            collect_named_types(schema.get("values", "null"), result)


def _push_named_children(node: dict[str, Any], stack: list[AvroType]) -> None:
    match node.get("type"):
        case "record":
            for field in node.get("fields", []):
                stack.append(field.get("type", "null"))
        case "array":
            stack.append(node.get("items", "null"))
        case "map":
            stack.append(node.get("values", "null"))


def find_named_deps(avro_type: AvroType) -> set[str]:
    """Return every named-type string reference found anywhere inside *avro_type*.

    These are the cross-schema dependencies used for topological sorting.
    Primitive type names are excluded.
    """
    deps: set[str] = set()
    stack: list[AvroType] = [avro_type]
    while stack:
        node = stack.pop()
        if isinstance(node, str) and node not in _PRIMITIVES:
            deps.add(node)
        elif isinstance(node, list):
            stack.extend(node)
        elif isinstance(node, dict):
            _push_named_children(node, stack)
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


def _schema_fqn(schema: dict[str, Any]) -> str:
    """Canonical dedup/visited key: ``namespace.Name`` if namespaced, else ``Name``."""
    name: str = schema["name"]
    namespace: str = schema.get("namespace", "")
    if namespace and "." not in name:
        return f"{namespace}.{name}"
    return name


def _topo_sort(
    named_types: list[dict[str, Any]],
    name_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return *named_types* ordered so every dependency precedes its dependant.

    Uses iterative post-order DFS to avoid recursion limits on deep schema chains.
    Schemas whose dependencies are not in *name_map* (external references) are
    silently ignored for ordering purposes.
    """
    result: list[dict[str, Any]] = []
    seen: set[str] = set()  # pushed to stack; prevents duplicate stack entries
    done: set[str] = set()  # appended to result; prevents duplicate output

    for start in named_types:
        stack: list[tuple[dict[str, Any], bool]] = [(start, False)]
        while stack:
            schema, post = stack.pop()
            key = _schema_fqn(schema)
            if post:
                if key not in done:
                    done.add(key)
                    result.append(schema)
            else:
                if key in done or key in seen:
                    continue
                seen.add(key)
                stack.append((schema, True))
                for dep_ref in sorted(find_named_deps(schema)):
                    dep = name_map.get(dep_ref)
                    if dep is not None and _schema_fqn(dep) != key:
                        stack.append((dep, False))

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
    """Parse a .avsc file.

    A file may contain a single schema object or a JSON array of schemas.
    """
    raw = json.loads(path.read_text())
    return raw if isinstance(raw, list) else [raw]


def _collect_deduped_named_types(
    schemas: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    named_types: list[dict[str, Any]] = []
    seen: set[str] = set()
    for schema in schemas:
        candidates: list[dict[str, Any]] = []
        collect_named_types(schema, candidates)
        for named in candidates:
            fqn = _schema_fqn(named)
            if fqn not in seen:
                seen.add(fqn)
                named_types.append(named)
    return named_types


def _gather_stdlib_imports(named_types: list[dict[str, Any]]) -> set[str]:
    stdlib_imports: set[str] = set()
    for named in named_types:
        if named.get("type") == "record":
            for field in named.get("fields", []):
                type_annotation(field["type"], stdlib_imports)
    return stdlib_imports


def _build_import_lines(stdlib_imports: set[str], has_enum: bool) -> list[str]:
    lines = ["from __future__ import annotations"]
    for mod in sorted(stdlib_imports):
        lines.append(f"import {mod}")
    if has_enum:
        lines.append("import enum")
    lines.append("from typing import Any, ClassVar")
    lines.append("from kafkian.base import AvroModel")
    return lines


def _render_blocks(
    named_types: list[dict[str, Any]],
    top_level_by_fqn: dict[str, dict[str, Any]],
) -> list[str]:
    blocks: list[str] = []
    for named in named_types:
        match named.get("type"):
            case "enum":
                blocks.append(_render_enum(named))
            case "record":
                avsc_dict = top_level_by_fqn.get(_schema_fqn(named), named)
                blocks.append(_render_record(named, avsc_dict))
    return blocks


def generate_from_dir(avsc_dir: Path) -> str:
    """Generate Pydantic models from all .avsc files in *avsc_dir*.

    Returns a Python module source string ready to be written to a file.
    """
    schemas: list[dict[str, Any]] = []
    for path in sorted(avsc_dir.glob("*.avsc")):
        schemas.extend(load_avsc(path))

    if not schemas:
        return "# No .avsc files found\n"

    named_types = _collect_deduped_named_types(schemas)
    named_types = _topo_sort(named_types, _build_name_map(named_types))

    stdlib_imports = _gather_stdlib_imports(named_types)
    has_enum = any(n.get("type") == "enum" for n in named_types)
    top_level_by_fqn = {_schema_fqn(s): s for s in schemas}

    import_lines = _build_import_lines(stdlib_imports, has_enum)
    blocks = _render_blocks(named_types, top_level_by_fqn)

    return "\n".join(import_lines) + "\n\n\n" + "\n\n\n".join(blocks) + "\n"

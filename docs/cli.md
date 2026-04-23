# CLI

`kafkian` ships a `generate` command that reads `.avsc` files and emits Python source with Pydantic models.

## Design decisions

**Topological sort** — `collect_named_types` does a depth-first traversal to collect all `record` and `enum` definitions. `_topo_sort` then orders them so every dependency precedes its dependant. Cross-file dependencies (a record in one `.avsc` referencing a type from another) are resolved via `_build_name_map`, which indexes every named type by all its alias forms.

**Code generation** — records become `class Foo(AvroModel):` with a `_schema: ClassVar[dict[str, Any]]` class variable holding the original AVSC dict (formatted with `pprint.pformat`). Enum schemas become `class Foo(str, enum.Enum):`. Imports are emitted only for stdlib modules actually referenced by the generated types.

**Single output module** — all types from all `.avsc` files in a directory are emitted into one Python source string, which callers can write to a file or pipe to stdout.

## Usage

```bash
# Print to stdout
kafkian generate ./schemas/

# Write to file
kafkian generate ./schemas/ -o kafkian/models.py
```

Or from Python:

```python
from kafkian.avsc_to_pydantic import generate_from_dir

source = generate_from_dir(Path("./schemas"))
Path("models.py").write_text(source)
```

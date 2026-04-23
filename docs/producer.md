# Producer

## Design decisions

**`AvroModel` base class** — all generated models inherit from `AvroModel(BaseModel)`, which is `frozen=True` and carries a `ClassVar[dict[str, Any]] _schema`. Storing the schema as a Python dict (rather than a JSON string or a `Schema` instance) keeps diffs readable and eliminates parsing round-trips; serialization to JSON happens once, at `Schema(json.dumps(...))` construction inside `build_schema`.

**`SchemaRegistry` facade** — wraps `SchemaRegistryClient` with two responsibilities:
1. *Model index* (`_model_registry`): maps every name alias (`SimpleName`, `namespace.SimpleName`, etc.) to its `AvroModel` subclass, enabling dependency auto-resolution.
2. *Registration cache* (`_registered: dict[str, int]`): subject → schema_id, so `ensure_registered` is idempotent within a process lifetime.

**Dependency traversal** — `ensure_registered` calls `find_named_deps` on the model's `_schema` dict, looks each dep up in `_model_registry`, and recursively calls `ensure_registered` on it before registering the current model. This guarantees topological order without requiring callers to manage it.

**Subject naming** — default subject is `{ModelClass.__name__}-value` (Python class name, not the Avro schema `name` field). This decouples the Python class name from the Avro namespace/name and is consistent with what `KafkianProducer` uses for serializer caching.

**Lazy serializer creation** — `KafkianProducer._get_serializer` calls `ensure_registered` + `build_schema` on first `produce()` for each model class, then caches the `AvroSerializer`. Subsequent calls skip both SR and serializer construction.

**`wait` parameter** — `produce(wait=True)` (the default) injects an internal delivery callback, polls until the broker acknowledges, and returns a `Message`. The `value` and `key` fields of the returned `Message` are taken from the arguments passed to `produce()` — not read back from the wire — since they are unchanged by delivery. Broker-assigned fields (`partition`, `offset`, `timestamp_ms`) come from the delivered `confluent_kafka.Message`. When `wait=False`, the call enqueues fire-and-forget and returns `None`; the caller is responsible for calling `flush()` or `poll()`. A `KafkaException` is raised on delivery error.

**`SchemaReference` wiring** — `_resolve_references` inspects the schema dict for named-type string references (e.g. `"com.example.Audit"`), fetches the latest registered version from SR, and returns `SchemaReference` objects. This is required by `confluent_kafka` when using cross-schema references.

## Key classes

| Class | Location | Purpose |
|---|---|---|
| `AvroModel` | `kafkian/base.py` | Frozen Pydantic base with `_schema: ClassVar[dict]` |
| `SchemaRegistry` | `kafkian/schema_registry.py` | Model index + registration cache + SR facade |
| `KafkianProducer` | `kafkian/producer.py` | Lazy serializer creation, key handling, produce delegation |

## Usage

```python
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from kafkian.schema_registry import SchemaRegistry
from kafkian.producer import KafkianProducer

sr = SchemaRegistry(SchemaRegistryClient({"url": "http://localhost:8081"}))
sr.register_model(AuditModel)  # index deps before dependants
sr.register_model(OrderCreatedModel)

producer = KafkianProducer(
    Producer({"bootstrap.servers": "localhost:9092"}),
    sr,
)
# wait=True (default): blocks until delivery, returns kafkian.Message
msg = producer.produce("orders", OrderCreatedModel(order_id="123"), key="123")
print(msg.partition, msg.offset)

# wait=False: fire-and-forget; flush manually
producer.produce("orders", OrderCreatedModel(order_id="456"), key="456", wait=False)
producer.flush()
```

`register_model` is optional if you call `ensure_registered` explicitly before producing; it is required for automatic dep traversal inside `ensure_registered`.

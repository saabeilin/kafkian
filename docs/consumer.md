# Consumer

## Design decisions

**`AvroDeserializer` with `return_record_name=True`** — the consumer uses a single shared `AvroDeserializer` configured with `return_record_name=True`, which returns `(record_name, dict)` tuples. This enables a generic dispatch loop: look up the record name in the `SchemaRegistry` model index, instantiate the matching `AvroModel` subclass, and yield a typed `Message`. No per-topic or per-schema deserializer setup is needed.

**Model index reuse** — `KafkianConsumer` relies on the same `SchemaRegistry._model_registry` used by the producer. Call `register_model()` for every type you expect to receive; the consumer looks up record names at decode time via the new public `lookup_model(name)` method.

**`unknown_schemas` policy** — what happens when a deserialized record name has no registered model is controlled by the `unknown_schemas` constructor parameter:

| Value | Behaviour |
|-------|-----------|
| `UnknownSchema.RAISE` (default) | Raises `LookupError` with topic/partition/offset context |
| `UnknownSchema.SKIP` | Logs a `WARNING` and silently drops the message |
| `UnknownSchema.RAW` | Yields the `Message` with `value` as `dict` (Avro) or `bytes` (non-Avro SR format) |

**Manual offset commits** — the consumer does not auto-commit. Call `commit_offset()` after processing each message (or batch) to advance the committed offset. This is the standard at-least-once processing pattern.

**`commit_offset` offset arithmetic** — Kafka's commit convention is "commit the next offset to consume", so `commit_offset` always commits `message.offset + 1` when deriving offsets from `Message` objects. When passing `TopicPartition` objects directly, the offsets are forwarded as-is.

**Context manager** — `KafkianConsumer` implements `__enter__`/`__exit__` so `consumer.close()` is called reliably in a `with` block, which triggers the underlying `Consumer.close()` (final offset flush, partition revocation callbacks, etc.).

## Key classes

| Class | Location | Purpose |
|---|---|---|
| `KafkianConsumer` | `kafkian2/consumer.py` | Poll loop, Avro decode, offset commit |
| `UnknownSchema` | `kafkian2/consumer.py` | Enum controlling behaviour for unregistered record names |
| `SchemaRegistry` | `kafkian2/schema_registry.py` | Model index via `register_model` / `lookup_model` |

## Usage

### Basic

```python
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from kafkian import KafkianConsumer, SchemaRegistry
from myapp.models import OrderCreatedModel, OrderCancelledModel

sr = SchemaRegistry(SchemaRegistryClient({"url": "http://localhost:8081"}))
sr.register_model(OrderCreatedModel)
sr.register_model(OrderCancelledModel)

raw_consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-processor",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # always disable auto-commit when using commit_offset()
})
raw_consumer.subscribe(["orders"])

with KafkianConsumer(raw_consumer, sr) as consumer:
    for message in consumer.consume():
        match message.value:
            case OrderCreatedModel() as evt:
                handle_created(evt)
            case OrderCancelledModel() as evt:
                handle_cancelled(evt)
        consumer.commit_offset(message)
```

### Skipping unknown schemas

```python
from kafkian import KafkianConsumer, UnknownSchema

with KafkianConsumer(raw_consumer, sr, unknown_schemas=UnknownSchema.SKIP) as consumer:
    for message in consumer.consume():
        process(message)
        consumer.commit_offset(message)
```

Unknown messages are logged at `WARNING` level via the `kafkian2.consumer` logger and silently dropped — the loop continues without yielding them.

### Batch commit

```python
batch: list[Message] = []
with KafkianConsumer(raw_consumer, sr) as consumer:
    for message in consumer.consume():
        batch.append(message)
        if len(batch) >= 100:
            process_batch(batch)
            consumer.commit_offset(batch)  # list[Message] accepted
            batch.clear()
```

### Explicit TopicPartition offsets

```python
from confluent_kafka import TopicPartition

consumer.commit_offset([TopicPartition("orders", partition=0, offset=42)])
```

### Customising the poll timeout

```python
for message in consumer.consume(timeout=0.5):
    ...
```

The default timeout is `1.0` second. Shorter values reduce latency at the cost of more CPU; longer values are fine for low-throughput topics.

# kafkian2 Usage

## 1. Generate models from Avro schemas

```bash
avsc2pydantic schemas/          # prints to stdout
avsc2pydantic schemas/ -o models.py
```

Each generated class extends `AvroModel` (a frozen Pydantic `BaseModel`) and carries its Avro schema as `_schema: ClassVar[str]`.

You can also call it from Python:

```python
from kafkian import generate_from_dir
from pathlib import Path

source = generate_from_dir(Path("schemas/"))
```

## 2. Register schemas

Create a `SchemaRegistry`, index your models, then call `ensure_registered`. **Register dependencies before the schemas that reference them.**

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from kafkian import SchemaRegistry
from myapp.models import AuditModel, OrderCreatedModel

sr = SchemaRegistry(SchemaRegistryClient({"url": "http://localhost:8081"}))
sr.register_model(AuditModel)  # index dep first
sr.register_model(OrderCreatedModel)  # index referencing schema

sr.ensure_registered(AuditModel)
sr.ensure_registered(OrderCreatedModel)  # auto-registers AuditModel if not yet done
```

`ensure_registered` is idempotent and caches results in memory — safe to call on every startup.

## 3. Produce messages

```python
from confluent_kafka import Producer
from kafkian import KafkianProducer

producer = KafkianProducer(
    Producer({"bootstrap.servers": "localhost:9092"}),
    sr,
)

order = OrderCreatedModel(order_id="abc-123", ...)
producer.produce("orders", order, key="abc-123")
producer.flush()
```

On the first `produce` for each model class, `KafkianProducer` calls `ensure_registered` automatically — no separate registration step needed if you prefer lazy registration.

## 4. Optional produce parameters

```python
producer.produce(
    "orders",
    order,
    key="abc-123",          # str (UTF-8 encoded) or bytes; omit for keyless
    partition=2,            # target partition
    headers={"trace": "x"}, # message headers
    on_delivery=callback,   # fn(KafkaError | None, Message) -> None
)
```

## 5. Consume messages

Register the models you expect to receive, then iterate `consume()`:

```python
from confluent_kafka import Consumer
from kafkian import KafkianConsumer

raw_consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-group",
    "enable.auto.commit": False,
})
raw_consumer.subscribe(["orders"])

with KafkianConsumer(raw_consumer, sr) as consumer:
    for message in consumer.consume():
        print(message.topic, message.value)  # value is an AvroModel subclass
        consumer.commit_offset(message)
```

`commit_offset` accepts a single `Message`, a `list[Message]`, or a `list[TopicPartition]`.

### Handling unknown schemas

```python
from kafkian import UnknownSchema

# RAISE (default) — LookupError on unregistered record
# SKIP  — logger warning and drop the message
# RAW   — yield with value as dict (Avro) or bytes (non-Avro)
consumer = KafkianConsumer(raw_consumer, sr, unknown_schemas=UnknownSchema.SKIP)
```

## 6. Consumer app (routing)

`KafkianApp` wraps a consumer and dispatches messages to handlers via a `@app.on(topic, SchemaType)` decorator — the same model as Flask/FastAPI URL routing.

```python
from typing import cast
from kafkian import KafkianApp, KafkianConsumer

raw_consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-service",
    "enable.auto.commit": False,
})
raw_consumer.subscribe(["orders", "payments"])

app = KafkianApp(KafkianConsumer(raw_consumer, sr))


@app.on("orders", OrderCreatedModel)
def handle_order(message: Message) -> None:
    order = cast(OrderCreatedModel, message.value)
    ...


@app.on("payments", PaymentProcessedModel)
def handle_payment(message: Message) -> None:
    ...


app.run()
```

Topic patterns follow shell-glob syntax (`*`, `?`). The most specific matching route wins (exact topic + exact schema beats wildcards). After a handler returns without raising, `commit_offset` is called automatically.

### Wildcard patterns

```python
@app.on("events.*")                        # any topic under events.*
def handle_any_event(message: Message) -> None:
    ...

@app.on("events.orders", OrderCreatedModel)  # more specific — wins when both match
def handle_order_event(message: Message) -> None:
    ...
```

### Router composition

```python
from kafkian import Router

billing_router = Router()


@billing_router.on("payments", PaymentProcessedModel)
def handle_payment(message: Message) -> None:
    ...


app.include_router(billing_router)
app.run()
```

### Custom error hooks

```python
app = KafkianApp(
    KafkianConsumer(raw_consumer, sr),
    on_unhandled=lambda msg: log.warning("no handler for %s", msg.topic),
    on_error=lambda msg, exc: log.exception("handler failed", exc_info=exc),
)
```

Set `auto_commit=False` and call `app.consumer.commit_offset(message)` manually for batching or transactional patterns.

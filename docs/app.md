# Consumer App

## Design decisions

**Flask-style routing** — `KafkianApp` maps `(topic_pattern, schema_type)` pairs to handler callables via a `@app.on()` decorator, the same mental model as URL routing in Flask or FastAPI. A standalone `Router` class holds registrations independently of a consumer, enabling Blueprint-style decomposition of handlers across modules.

**`fnmatch` topic patterns** — topic patterns follow shell-glob syntax (`*`, `?`, `[seq]`) via `fnmatch.fnmatchcase`. This gives `"events.*"` (matches `"events.orders"`, `"events.payments"`) and `"orders.?"` (matches any single-character suffix) without introducing regex complexity. Patterns are case-sensitive.

**`isinstance` schema matching** — schema type is matched with `isinstance(message.value, schema_type)`, which naturally handles subclassing. Passing `schema_type=None` (the default) matches any value type on that topic.

**Specificity scoring** — when multiple routes match a message, the most specific wins. Scores are computed once at registration time:

| topic_pattern | schema_type | score |
|---|---|---|
| exact (no `*?[`) | exact type | 4 |
| exact | `None` | 3 |
| wildcard | exact type | 2 |
| wildcard | `None` | 1 |

First-registered wins among routes with equal scores.

**Handler signature** — handlers always receive the full `Message` object. This gives access to topic, key, headers, partition, offset, and timestamp alongside the decoded value, all of which are commonly needed for tracing, conditional routing, and DLQ logic.

**`auto_commit=True` by default** — after a handler returns without raising, `commit_offset(message)` is called automatically. Set `auto_commit=False` to manage offsets inside the handler (e.g. for batching or transactional patterns); the consumer is accessible via `app.consumer`.

**Error isolation** — if a handler raises, the exception is passed to `on_error` and the consume loop continues. The offset for that message is not committed. The default `on_error` logs the exception at `ERROR` level; the default `on_unhandled` logs at `WARNING` level.

**Router composition** — `include_router(sub_router)` merges another router's routes into the app. Merged routes are appended after the app's own routes, so app-level handlers take precedence on ties.

## Key classes

| Class | Location | Purpose |
|---|---|---|
| `KafkianApp` | `kafkian2/app.py` | Consume loop, dispatch, offset commit, error hooks |
| `Router` | `kafkian2/app.py` | Standalone route registry for Blueprint-style composition |
| `RouteEntry` | `kafkian2/app.py` | Frozen dataclass holding one registered route + pre-computed score |
| `HandlerFunc` | `kafkian2/app.py` | Type alias: `Callable[[Message], None]` |

## Usage

### Basic

```python
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from kafkian import KafkianApp, KafkianConsumer, SchemaRegistry
from myapp.models import OrderCreatedModel, PaymentProcessedModel

sr = SchemaRegistry(SchemaRegistryClient({"url": "http://localhost:8081"}))
sr.register_model(OrderCreatedModel)
sr.register_model(PaymentProcessedModel)

raw_consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-service",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
raw_consumer.subscribe(["orders", "payments"])

app = KafkianApp(KafkianConsumer(raw_consumer, sr))


@app.on("orders", OrderCreatedModel)
def handle_order(message: Message) -> None:
    order = cast(OrderCreatedModel, message.value)
    print(f"order {order.order_id} on partition {message.partition}")


@app.on("payments", PaymentProcessedModel)
def handle_payment(message: Message) -> None:
    ...


app.run()
```

### Wildcard topic patterns

```python
# Matches any topic under the "events." prefix
@app.on("events.*")
def handle_any_event(message: Message) -> None:
    log.info("event on %s: %s", message.topic, type(message.value).__name__)

# Exact topic + exact schema takes precedence (score 4 vs 1)
@app.on("events.orders", OrderCreatedModel)
def handle_order_event(message: Message) -> None:
    ...
```

### Router composition

Define handlers in sub-modules and merge them into the app:

```python
# billing/handlers.py
from kafkian import Router

billing_router = Router()


@billing_router.on("payments", PaymentProcessedModel)
def handle_payment(message: Message) -> None:
    ...
```

```python
# main.py
from billing.handlers import billing_router

app = KafkianApp(KafkianConsumer(raw_consumer, sr))
app.include_router(billing_router)
app.run()
```

Routes from `include_router` are appended after the app's own routes. Register app-level handlers before calling `include_router` if they should have priority on ties.

### Custom error hooks

```python
def on_unhandled(message: Message) -> None:
    dlq_producer.produce("dlq.unrouted", UnroutedEvent(topic=message.topic))

def on_error(message: Message, exc: Exception) -> None:
    log.exception("handler failed", exc_info=exc)
    dlq_producer.produce("dlq.errors", FailedEvent(topic=message.topic))

app = KafkianApp(
    KafkianConsumer(raw_consumer, sr),
    on_unhandled=on_unhandled,
    on_error=on_error,
)
```

### Manual offset management

```python
app = KafkianApp(KafkianConsumer(raw_consumer, sr), auto_commit=False)

@app.on("orders", OrderCreatedModel)
def handle_order(message: Message) -> None:
    db.save(message.value)
    app.consumer.commit_offset(message)   # commit only after successful save
```

### Typing the handler value

The `message.value` field is typed as `bytes | dict | AvroModel | None`. Use `cast` inside the handler for a typed local variable:

```python
from typing import cast

@app.on("orders", OrderCreatedModel)
def handle_order(message: Message) -> None:
    order = cast(OrderCreatedModel, message.value)
    ...
```

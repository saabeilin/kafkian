# Changelog

## 0.1.0 (unreleased)

Initial release.

- `AvroModel`: frozen Pydantic base class with `_schema: ClassVar[dict[str, Any]]`
- `SchemaRegistry`: model index + registration cache + `SchemaReference` wiring; new `lookup_model(name)` public method
- `KafkianProducer`: lazy serializer creation, key handling, optional `partition`/`headers`/`on_delivery`
- `KafkianConsumer`: Avro decode via `AvroDeserializer(return_record_name=True)`, generator-based `consume()`, manual `commit_offset()`, configurable `unknown_schemas` policy (`raise`/`skip`/`raw`), context manager support
- `UnknownSchema`: enum controlling behaviour for messages with unregistered record names
- `kafkian generate` CLI: `.avsc` → Pydantic model source with topological sort
- `Router`: standalone route registry mapping `(topic_pattern, schema_type)` → handler via `@router.on()` decorator; `include_router()` for Blueprint-style composition
- `KafkianApp`: consumer app wrapping `KafkianConsumer` with routing, specificity-based dispatch (`fnmatch` topic patterns, `isinstance` schema matching), `auto_commit`, and `on_unhandled`/`on_error` hooks

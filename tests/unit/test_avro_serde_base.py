from confluent_kafka import avro

from kafkian.serde.avroserdebase import HasSchemaMixin, _wrap


RECORD_SCHEMA = avro.loads("""
{
    "name": "SomethingHappened",
    "type": "record",
    "doc": "basic schema for tests",
    "namespace": "python.test.basic",
    "fields": [
        {
            "name": "number",
            "doc": "age",
            "type": [
                "long",
                "null"
            ]
        },
        {
            "name": "name",
            "doc": "a name",
            "type": [
                "string"
            ]
        }
    ]
}
""")


KEY_SCHEMA = avro.loads("""{"type": "string"}""")


def test_schema_mixin_wrapper_record_schema():
    for base_class in (int, float, dict, list):
        val = base_class()
        wrapped = _wrap(val, RECORD_SCHEMA)
        assert val == wrapped
        assert isinstance(wrapped, base_class)
        assert isinstance(wrapped, HasSchemaMixin)
        assert wrapped.schema is RECORD_SCHEMA
        assert wrapped.__class__.__name__ == 'python.test.basic.SomethingHappened'


def test_schema_mixin_wrapper_key_schema():
    for base_class in (int, float, dict, list):
        val = base_class()
        wrapped = _wrap(val, KEY_SCHEMA)
        assert val == wrapped
        assert isinstance(wrapped, base_class)
        assert isinstance(wrapped, HasSchemaMixin)
        assert wrapped.schema is KEY_SCHEMA
        assert wrapped.__class__.__name__ == KEY_SCHEMA.type

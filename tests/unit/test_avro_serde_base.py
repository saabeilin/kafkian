from confluent_kafka import avro

from kafkian.serde.avroserdebase import HasSchemaMixin, _wrap


SCHEMA = avro.loads("""
{
    "name": "basic",
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


def test_schema_mixin_wrapper():
    for base_class in (int, float, dict, list):
        val = base_class()
        wrapped = _wrap(val, SCHEMA)
        assert val == wrapped
        assert isinstance(wrapped, base_class)
        assert isinstance(wrapped, HasSchemaMixin)
        assert wrapped.schema() is SCHEMA
        assert wrapped.__class__.__name__ == 'python.test.basic.basic'

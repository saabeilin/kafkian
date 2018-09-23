import uuid

import pytest
from confluent_kafka import avro

from kafkian import Producer, Consumer
from kafkian.serde.deserialization import AvroDeserializer
from kafkian.serde.serialization import AvroStringKeySerializer, AvroSerializer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'http://localhost:28081'
TEST_TOPIC = 'test.test.' + str(uuid.uuid4())

CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'default.topic.config': {
        'auto.offset.reset': 'earliest',
    },
    'group.id': str(uuid.uuid4())
}

PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
}


class Message(dict):
    _schema = None


value_schema_str = """
{
   "namespace": "my.test",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

Message._schema = avro.loads(value_schema_str)


@pytest.fixture
def producer():
    return Producer(
        PRODUCER_CONFIG,
        key_serializer=AvroStringKeySerializer(schema_registry_url=SCHEMA_REGISTRY_URL),
        value_serializer=AvroSerializer(schema_registry_url=SCHEMA_REGISTRY_URL)
    )


@pytest.fixture
def consumer():
    return Consumer(
        CONSUMER_CONFIG,
        [TEST_TOPIC],
        key_deserializer=AvroDeserializer(schema_registry_url=SCHEMA_REGISTRY_URL),
        value_deserializer=AvroDeserializer(schema_registry_url=SCHEMA_REGISTRY_URL)
    )


def test_produce_consume_one(producer, consumer):
    key = str(uuid.uuid4())
    value = Message({
        'name': 'some name'
    })

    producer.produce(TEST_TOPIC, key, value, sync=True)
    m = next(consumer)
    assert m.key() == key
    assert m.value() == value

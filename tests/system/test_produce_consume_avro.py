import uuid

import pytest
from confluent_kafka import avro

from kafkian import Consumer, Producer
from kafkian.serde.avroserdebase import AvroRecord
from kafkian.serde.deserialization import AvroDeserializer
from kafkian.serde.serialization import AvroSerializer, AvroStringKeySerializer

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:28081"
TEST_TOPIC = "test.test." + str(uuid.uuid4())

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "auto.offset.reset": "earliest",
    "group.id": str(uuid.uuid4()),
}

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
}


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


class Message(AvroRecord):
    _schema = avro.loads(value_schema_str)


@pytest.fixture
def producer():
    return Producer(
        PRODUCER_CONFIG,
        key_serializer=AvroStringKeySerializer(schema_registry_url=SCHEMA_REGISTRY_URL),
        value_serializer=AvroSerializer(schema_registry_url=SCHEMA_REGISTRY_URL),
    )


@pytest.fixture
def consumer():
    return Consumer(
        CONSUMER_CONFIG,
        [TEST_TOPIC],
        key_deserializer=AvroDeserializer(schema_registry_url=SCHEMA_REGISTRY_URL),
        value_deserializer=AvroDeserializer(schema_registry_url=SCHEMA_REGISTRY_URL),
    )


def test_produce_consume_one(producer, consumer):
    key = str(uuid.uuid4())
    value = Message({"name": "some name"})

    producer.produce(TEST_TOPIC, key, value, sync=True)
    with consumer:
        m = next(consumer)
        consumer.commit(sync=True)
    assert m.key == key
    assert m.value == value


def test_produce_consume_one_tombstone(producer, consumer):
    key = str(uuid.uuid4())
    value = None

    producer.produce(TEST_TOPIC, key, value, sync=True)
    with consumer:
        m = next(consumer)
        consumer.commit(sync=True)
    assert m.key == key
    assert m.value == value

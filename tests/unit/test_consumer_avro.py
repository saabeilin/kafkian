import uuid
from unittest.mock import Mock, patch

import pytest
from confluent_kafka import avro

from kafkian import Consumer
from kafkian.serde.avroserdebase import AvroRecord
from kafkian.serde.deserialization import AvroDeserializer
from kafkian.serde.serialization import AvroSerializer

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TEST_TOPIC = "test.test." + str(uuid.uuid4())
SCHEMA_REGISTRY_URL = "mock://"

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "auto.offset.reset": "earliest",
    "group.id": str(uuid.uuid4()),
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


message = Message({"name": "some name"})


@pytest.fixture
def serializer(mock_schema_registry_client):
    return AvroSerializer(schema_registry_client=mock_schema_registry_client)


@pytest.fixture
def deserializer(mock_schema_registry_client):
    return AvroDeserializer(schema_registry_client=mock_schema_registry_client)


@pytest.fixture
def consumer(mock_schema_registry_client):
    return Consumer(
        CONSUMER_CONFIG,
        [TEST_TOPIC],
        schema_registry_client=mock_schema_registry_client,
    )


class MockMessage(Mock):
    def key(self):
        return self._key

    def value(self):
        return self._value

    def set_key(self, new_key):
        self._key = new_key

    def set_value(self, new_value):
        self._value = new_value

    def error(self):
        return None


def test_consume_one_avro_value(consumer, serializer):
    key = bytes(str(uuid.uuid4()), encoding="utf8")
    value = message

    m = MockMessage()
    m.set_key(key)
    m.set_value(serializer.serialize(value, TEST_TOPIC))

    with patch("kafkian.consumer.Consumer._poll", Mock(return_value=m)):
        received = next(consumer)
    assert received.key == key
    assert received.value == value


def test_consume_one_tombstone(consumer):
    key = bytes(str(uuid.uuid4()), encoding="utf8")
    value = None

    m = MockMessage()
    m.set_key(key)
    m.set_value(value)

    with patch("kafkian.consumer.Consumer._poll", Mock(return_value=m)):
        m = next(consumer)
    assert m.key == key
    assert m.value == value

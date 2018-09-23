import uuid
from unittest.mock import MagicMock, patch

import pytest

from kafkian import producer
from kafkian.serde.serialization import Serializer, AvroStringKeySerializer
from tests.unit.conftest import producer_produce_mock

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'https://localhost:28081'
TEST_TOPIC = 'test.test.' + str(uuid.uuid4())

PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'schema.registry.url': SCHEMA_REGISTRY_URL,
}


def teardown_function(function):
    producer_produce_mock.reset_mock()


@pytest.fixture(scope='module')
def avro_producer():
    return producer.Producer(
        PRODUCER_CONFIG,
        key_serializer=AvroStringKeySerializer(schema_registry_url=SCHEMA_REGISTRY_URL)
    )


def test_producer_init(avro_producer):
    assert isinstance(
        avro_producer.key_serializer,
        AvroStringKeySerializer
    )
    assert isinstance(
        avro_producer.value_serializer,
        Serializer
    )


@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.register',
    MagicMock(return_value=1)
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_latest_schema',
    MagicMock(return_value=(1, AvroStringKeySerializer.KEY_SCHEMA, 1))
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_by_id',
    MagicMock(return_value=AvroStringKeySerializer.KEY_SCHEMA)
)
def test_avro_producer_produce(avro_producer):
    key = 'a'
    value = 'a'
    topic = 'c'
    avro_producer.produce(topic, key=key, value=value)

    producer_produce_mock.assert_called_once_with(
        topic,
        b'\x00\x00\x00\x00\x01\x02a',
        value
    )

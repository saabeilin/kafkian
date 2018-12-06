import uuid
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import avro

from kafkian import producer
from kafkian.serde.avroserdebase import AvroRecord
from kafkian.serde.serialization import AvroSerializer, Serializer
from tests.unit.conftest import producer_produce_mock

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'https://localhost:28081'
TEST_TOPIC = 'test.test.' + str(uuid.uuid4())

PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'schema.registry.url': SCHEMA_REGISTRY_URL,
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


message = Message({
    'name': 'some name'
})


def teardown_function(function):
    producer_produce_mock.reset_mock()


@pytest.fixture(scope='module')
def avro_producer():
    return producer.Producer(
        PRODUCER_CONFIG,
        value_serializer=AvroSerializer(schema_registry_url=SCHEMA_REGISTRY_URL)
    )


def test_producer_init(avro_producer):
    assert isinstance(
        avro_producer.key_serializer,
        Serializer
    )
    assert isinstance(
        avro_producer.value_serializer,
        AvroSerializer
    )


@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.register',
    MagicMock(return_value=1)
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_latest_schema',
    MagicMock(return_value=(1, message._schema, 1))
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_by_id',
    MagicMock(return_value=message._schema)
)
def test_avro_producer_produce(avro_producer):
    key = 'a'
    value = message
    topic = 'z'
    avro_producer.produce(key=key, value=value, topic=topic)

    producer_produce_mock.assert_called_once_with(
        topic,
        key,
        avro_producer.value_serializer.serialize(value, topic)
    )

#
# def test_get_subject_names(avro_producer):
#     topic_name = 'test_topic'
#     key_subject_name, value_subject_name = (
#         avro_producer._get_subject_names(topic_name)
#     )
#     assert key_subject_name == (topic_name + '-key')
#     assert value_subject_name == (topic_name + '-value')
#
#
# def test_get_topic_schemas(avro_producer):
#     mock_avro_schema_registry.return_value.\
#         get_latest_schema.side_effect = ['1', '2']
#     topic_list = ['a']
#     topic_schemas = avro_producer._get_topic_schemas(topic_list)
#     topic, key_schema, value_schema = topic_schemas['a']
#     assert topic == 'a'
#     assert key_schema == '1'
#     assert value_schema == '2'

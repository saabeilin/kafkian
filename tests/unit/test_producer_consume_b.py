import uuid

import pytest

from kafkian import Producer, Consumer
from tests.unit.conftest import producer_produce_mock, producer_flush_mock

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'https://localhost:28081'
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
    'schema.registry.url': SCHEMA_REGISTRY_URL,
}


@pytest.fixture
def producer():
    return Producer(CONSUMER_CONFIG)


@pytest.fixture
def consumer():
    return Consumer(CONSUMER_CONFIG, [TEST_TOPIC])


def test_produce_consume_one(producer, consumer):
    key = bytes(str(uuid.uuid4()), encoding='utf8')
    value = bytes(str(uuid.uuid4()), encoding='utf8')
    producer.produce(TEST_TOPIC, key, value, sync=True)

    producer_produce_mock.assert_called_once_with(TEST_TOPIC, key, value)
    producer_flush_mock.assert_called_once_with()

    # # producer.poll()
    # # producer.flush()
    # # producer.poll()
    # m = next(consumer)
    # assert m.key() == key
    # assert m.value() == value

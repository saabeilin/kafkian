import uuid

import pytest

from kafkian import Producer, Consumer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
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


@pytest.fixture
def producer():
    return Producer(PRODUCER_CONFIG)


@pytest.fixture
def consumer():
    return Consumer(CONSUMER_CONFIG, [TEST_TOPIC])


def test_produce_consume_one(producer, consumer):
    key = bytes(str(uuid.uuid4()), encoding='utf8')
    value = bytes(str(uuid.uuid4()), encoding='utf8')
    producer.produce(TEST_TOPIC, key, value, sync=True)
    m = next(consumer)
    assert m.key() == key
    assert m.value() == value

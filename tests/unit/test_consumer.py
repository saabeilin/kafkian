import uuid
from unittest.mock import patch, Mock

import pytest

from kafkian import Consumer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
TEST_TOPIC = 'test.test.' + str(uuid.uuid4())

CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'default.topic.config': {
        'auto.offset.reset': 'earliest',
    },
    'group.id': str(uuid.uuid4())
}


@pytest.fixture
def consumer():
    return Consumer(CONSUMER_CONFIG, [TEST_TOPIC])


class MockMessage(Mock):
    def key(self):
        return self._key

    def value(self):
        return self._value

    def set_key(self, new_key):
        self._key = new_key

    def set_value(self, new_value):
        self._value = new_value


def test_consume_one_b(consumer):
    key = bytes(str(uuid.uuid4()), encoding='utf8')
    value = bytes(str(uuid.uuid4()), encoding='utf8')

    m = MockMessage()
    m.set_key(key)
    m.set_value(value)

    with patch('kafkian.consumer.Consumer._poll', Mock(return_value=m)):
        m = next(consumer)
    assert m.key() == key
    assert m.value() == value


def test_consume_one_tombstone(consumer):
    key = bytes(str(uuid.uuid4()), encoding='utf8')
    value = None

    m = MockMessage()
    m.set_key(key)
    m.set_value(value)

    with patch('kafkian.consumer.Consumer._poll', Mock(return_value=m)):
        m = next(consumer)
    assert m.key() == key
    assert m.value() == value

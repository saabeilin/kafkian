import uuid
from unittest.mock import patch, Mock

import pytest
from confluent_kafka.cimpl import KafkaError

from tests.unit.conftest import consumer_close_mock
from kafkian.consumer import Consumer, KafkianException

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
TEST_TOPIC = 'test.test.' + str(uuid.uuid4())

CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'auto.offset.reset': 'earliest',
    'group.id': str(uuid.uuid4())
}


@pytest.fixture
def consumer():
    return Consumer(CONSUMER_CONFIG, [TEST_TOPIC])


class MockMessage:
    def __init__(self, _key, _value, _error):
        self._key = _key
        self._value = _value
        self._error = _error

    def key(self):
        return self._key

    def value(self):
        return self._value

    def set_key(self, new_key):
        self._key = new_key

    def set_value(self, new_value):
        self._value = new_value

    def error(self):
        return self._error


class MockError:
    def __init__(self, _code):
        self._code = _code

    def code(self):
        return self._code


def test_consumer_ignores_partition_eof(consumer):
    key = bytes(str(uuid.uuid4()), encoding='utf8')
    value = bytes(str(uuid.uuid4()), encoding='utf8')

    # The first message with _PARTITION_EOF error, should be skipped
    m1 = MockMessage(None, None, _error=MockError(_code=KafkaError._PARTITION_EOF))
    # The next one should be delivered
    m2 = MockMessage(key, value, _error=None)

    messages = iter([m1, m2])

    def next_message(ignored):
        return next(messages)

    with patch('kafkian.consumer.Consumer._poll', next_message):
        m = next(consumer)
    assert m.key == key
    assert m.value == value


def test_consumer_generator_raises_and_closed_on_error(consumer):
    m = MockMessage(None, None, _error=MockError(_code=-1))

    with patch('kafkian.consumer.Consumer._poll', Mock(return_value=m)):
        try:
            next(consumer)
        except Exception as e:
            assert isinstance(e, KafkianException)

        consumer_close_mock.assert_called_once_with()

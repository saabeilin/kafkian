import uuid

import pytest

from kafkian import Producer, Consumer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
TEST_TOPIC = 'test.test.' + str(uuid.uuid4())

CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'auto.offset.reset': 'earliest',
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


def test_produce_many_consume_one(producer, consumer):
    key = bytes(str(uuid.uuid4()), encoding='utf8')
    values = [bytes(str(uuid.uuid4()), encoding='utf8') for _ in range(10)]
    for value in values:
        producer.produce(TEST_TOPIC, key, value)
    producer.flush()
    with consumer:
        m = next(consumer)
        committed_offsets = consumer.commit(sync=True)
        committed = list(filter(lambda tp: tp.partition == m.partition and tp.topic == m.topic, committed_offsets))[0]
        assert committed.offset == m.offset + 1


def test_produce_many_consume_some(producer, consumer):
    key = bytes(str(uuid.uuid4()), encoding='utf8')
    values = [bytes(str(uuid.uuid4()), encoding='utf8') for _ in range(10)]
    for value in values:
        producer.produce(TEST_TOPIC, key, value)
    producer.flush()
    with consumer:
        for _ in range(len(values) // 2):
            m = next(consumer)
        committed_offsets = consumer.commit(sync=True)
        committed = list(filter(lambda tp: tp.partition == m.partition and tp.topic == m.topic, committed_offsets))[0]
        assert committed.offset == m.offset + 1

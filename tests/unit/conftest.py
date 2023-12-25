from unittest.mock import Mock, patch

producer_produce_mock = Mock()
producer_poll_mock = Mock(return_value=1)
producer_flush_mock = Mock()
consumer_close_mock = Mock()

mocks = [
    # patch('datadog.statsd', Mock()),
    patch("kafkian.producer.Producer._init_producer_impl", Mock(return_value=Mock())),
    patch("kafkian.Producer._produce", producer_produce_mock),
    patch("kafkian.Producer.poll", producer_poll_mock),
    patch("kafkian.Producer.flush", producer_flush_mock),
    patch("kafkian.consumer.Consumer._init_consumer_impl", Mock(return_value=Mock())),
    patch("kafkian.consumer.Consumer._close", consumer_close_mock),
]

for mock in mocks:
    mock.start()

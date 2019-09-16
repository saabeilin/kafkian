from typing import Optional

from confluent_kafka.cimpl import Message as ConfluentKafkaMessage, TIMESTAMP_NOT_AVAILABLE

from kafkian.serde.deserialization import Deserializer


class Message:
    """
    Message is an object (log record) consumed from Kafka.

    It provides read-only access to key, value, and message metadata:
    topic, partition, offset, and optionally timestamp and headers.

    Key and value and deserialized on first access.

    This class wraps cimpl.Message from confluent_kafka
    and not supposed to be user-instantiated.
    """
    def __init__(self, 
                 message: ConfluentKafkaMessage, 
                 key_deserializer: Deserializer,
                 value_deserializer: Deserializer):
        self._message = message
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        self._deserialized_key = None
        self._deserialized_value = None
    
    @property
    def key(self):
        """
        :return: Deserialized message key
        """
        if self._deserialized_key:
            return self._deserialized_key
        if self._message.key() is None:
            return None
        self._deserialized_key = self._key_deserializer.deserialize(self._message.key())
        return self._deserialized_key
    
    @property
    def value(self):
        """
        :return: Deserialized message value
        """
        if self._deserialized_value:
            return self._deserialized_value
        if self._message.value() is None:
            return None
        self._deserialized_value = self._value_deserializer.deserialize(self._message.value())
        return self._deserialized_value

    @property
    def topic(self) -> str:
        """
        :return: Message topic
        """
        return self._message.topic()

    @property
    def partition(self) -> int:
        """
        :return: Message partition
        """
        return self._message.partition()

    @property
    def offset(self) -> int:
        """
        :return: Message offset
        """
        return self._message.offset()

    @property
    def timestamp(self) -> Optional[int]:
        """
        :return: Message timestamp, of None if not available.
        """
        if not self._message.timestamp():
            return None
        if self._message.timestamp()[0] == TIMESTAMP_NOT_AVAILABLE:
            return None
        return self._message.timestamp()[1]

    @property
    def timestamp_type(self) -> Optional[int]:
        """
        :return: Message timestamp type - either message creation time or Log Append time, of None if not available.
        """
        if not self._message.timestamp():
            return None
        return self._message.timestamp()[0]

    @property
    def headers(self) -> list:
        """
        :return: Message headers as list of two-tuples, one (key, value) pair for each header.
        :rtype: [(str, bytes),...] or None.
        """
        return self._message.headers() or []

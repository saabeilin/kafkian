from confluent_kafka.cimpl import Message as ConfluentKafkaMessage

from kafkian.serde.deserialization import Deserializer


class Message:
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
        if self._deserialized_key:
            return self._deserialized_key
        if self._message.key() is None:
            return None
        self._deserialized_key = self._key_deserializer.deserialize(self._message.key())
        return self._deserialized_key
    
    @property
    def value(self):
        if self._deserialized_value:
            return self._deserialized_value
        if self._message.value() is None:
            return None
        self._deserialized_value = self._value_deserializer.deserialize(self._message.value())
        return self._deserialized_value

    @property
    def topic(self):
        return self._message.topic()

    @property
    def partition(self):
        return self._message.partition()

    @property
    def offset(self):
        return self._message.offset()

    @property
    def timestamp(self):
        return self._message.timestamp()

    @property
    def headers(self):
        return self._message.headers()

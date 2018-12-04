import struct

import avro.io
from confluent_kafka.avro import MessageSerializer as ConfluentMessageSerializer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro.serializer.message_serializer import ContextStringIO

MAGIC_BYTE = 0


class HasSchemaMixin:
    """
    A mixing for decoded Avro record to make able to add schema attribute
    """
    @property
    def schema(self):
        """
        :return: Avro schema for used to decode this entity
        :rtype: avro.schema.Schema
        """
        return self._schema


class AvroRecord(dict, HasSchemaMixin):
    pass


def _wrap(value, schema):
    """
    Wraps a value into subclass with HasSchemaMixin
    :param value: a decoded value
    :param schema: corresponding Avro schema used to decode value
    :return: An instance of a dynamically created class with schema fullname
    """
    if hasattr(schema, 'fullname'):
        name = schema.fullname
    elif hasattr(schema, 'namespace'):
        name = "{namespace}.{name}".format(namespace=schema.namespace, name=schema.name)
    elif hasattr(schema, 'name'):
        name = schema.name
    else:
        name = schema.type

    new_class = type(str(name), (value.__class__, HasSchemaMixin), {})

    wrapped = new_class(value)
    wrapped._schema = schema
    return wrapped


class AvroSerDeBase(ConfluentMessageSerializer):
    """
    A subclass of MessageSerializer from Confluent's kafka-python,
    adding schema to deserialized Avro messages.
    """

    def decode_message(self, message):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.
        @:param: message
        """

        if message is None:
            return None

        if len(message) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextStringIO(message) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")
            decoder_func = self._get_decoder_func(schema_id, payload)
            return _wrap(
                decoder_func(payload),
                self.registry_client.get_by_id(schema_id)
            )

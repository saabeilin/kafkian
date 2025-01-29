from confluent_kafka.schema_registry import SchemaRegistryClient

from .avroserdebase import AvroSerDeBase


class Deserializer:
    """Base class for all key and value deserializers.
    This default implementation returns the value intact.
    """

    def __init__(self, **kwargs):
        pass

    def deserialize(self, value, **kwargs):
        return value


class AvroDeserializer(Deserializer):
    def __init__(self, schema_registry_client: SchemaRegistryClient, **kwargs) -> None:
        super().__init__(**kwargs)
        self.schema_registry = schema_registry_client
        self._serializer_impl = AvroSerDeBase(self.schema_registry)

    def deserialize(self, value, **kwargs):
        return self._serializer_impl.decode_message(value)

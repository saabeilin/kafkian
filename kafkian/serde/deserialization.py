from confluent_kafka.avro import CachedSchemaRegistryClient, MessageSerializer


class Deserializer:
    """
    Base class for all key and value deserializers.
    This default implementation returns the value intact.
    """
    def __init__(self, **kwargs):
        pass

    def deserialize(self, value, **kwargs):
        return value


class AvroDeserializer(Deserializer):
    def __init__(self, schema_registry_url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.schema_registry = CachedSchemaRegistryClient(schema_registry_url)
        self._serializer_impl = MessageSerializer(self.schema_registry)

    def deserialize(self, value, **kwargs):
        return self._serializer_impl.decode_message(value)

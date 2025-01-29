import abc
from enum import Enum

from confluent_kafka import avro
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    record_subject_name_strategy,
    topic_record_subject_name_strategy,
    topic_subject_name_strategy,
)

from .avroserdebase import AvroRecord, AvroSerDeBase


class SubjectNameStrategy(Enum):
    TopicNameStrategy = topic_subject_name_strategy
    RecordNameStrategy = record_subject_name_strategy
    TopicRecordNameStrategy = topic_record_subject_name_strategy


class Serializer:
    """Base class for all key and value serializers.
    This default implementation returns the value intact.
    """

    def __init__(self, **kwargs):
        pass

    def serialize(self, value, topic, **kwargs):
        return value


class AvroSerializerBase(Serializer):
    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        auto_register_schemas: bool = True,
        subject_name_strategy: SubjectNameStrategy = SubjectNameStrategy.RecordNameStrategy,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.schema_registry = schema_registry_client
        self.auto_register_schemas = auto_register_schemas
        self.subject_name_strategy = subject_name_strategy
        self._serializer_impl = AvroSerDeBase(self.schema_registry)

    def _get_subject(self, topic: str, schema, is_key=False):
        if self.subject_name_strategy == SubjectNameStrategy.TopicNameStrategy:
            subject = topic + ("-key" if is_key else "-value")
        elif self.subject_name_strategy == SubjectNameStrategy.RecordNameStrategy:
            subject = schema.fullname
        elif self.subject_name_strategy == SubjectNameStrategy.TopicRecordNameStrategy:
            subject = f"{topic}-{schema.fullname}"
        else:
            raise ValueError("Unknown SubjectNameStrategy")
        return subject

    def _ensure_schema(self, topic: str, schema, is_key=False):
        subject = self._get_subject(topic, schema, is_key)

        if self.auto_register_schemas:
            schema_id = self.schema_registry.register_schema(subject, schema)
            schema = self.schema_registry.get_schema(schema_id)
        else:
            latest_schema = self.schema_registry.get_latest_with_metadata(subject)
            if latest_schema is None:
                raise ValueError(f"Schema not found for subject {subject}")
            schema_id = latest_schema.schema_id
            schema = latest_schema.schema

        return schema_id, schema

    @abc.abstractmethod
    def serialize(self, value, topic, **kwargs):
        raise NotImplementedError


class AvroSerializer(AvroSerializerBase):
    def serialize(self, value: AvroRecord, topic: str, is_key=False, **kwargs):
        schema_id, _ = self._ensure_schema(topic, value.schema, is_key)
        return self._serializer_impl.encode_record_with_schema_id(
            schema_id, value, is_key
        )


class AvroStringKeySerializer(AvroSerializerBase):
    """A specialized serializer for generic String keys,
    serialized with a simple value avro schema.
    """

    KEY_SCHEMA = avro.loads("""{"type": "string"}""")

    def serialize(self, value: str, topic: str, is_key=False, **kwargs):
        assert is_key
        schema_id, _ = self._ensure_schema(topic, self.KEY_SCHEMA, is_key)
        return self._serializer_impl.encode_record_with_schema_id(
            schema_id, value, is_key
        )

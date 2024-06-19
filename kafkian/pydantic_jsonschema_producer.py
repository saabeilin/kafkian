import json
import typing

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel

from kafkian import Producer
from kafkian.serde.serialization import Serializer


class PydanticJsonSchemaSerializer(Serializer):
    def serialize(self, value: BaseModel, topic: str, **kwargs) -> bytes:
        serializer = JSONSerializer(
            json.dumps(value.model_json_schema(mode="serialization")),
            SchemaRegistryClient({"url": "http://localhost:28081"}),
        )

        ser_context = SerializationContext(
            topic,
            MessageField.VALUE,
        )

        return serializer(value.model_dump(mode="json"), ser_context)


class PydanticJsonSchemaProducer(Producer):
    def __init__(
        self,
        config: dict,
        error_callback: typing.Callable | None = None,
        delivery_success_callback: typing.Callable | None = None,
        delivery_error_callback: typing.Callable | None = None,
        metrics=None,
    ) -> None:
        super().__init__(
            config=config,
            value_serializer=PydanticJsonSchemaSerializer(),
            error_callback=error_callback,
            delivery_success_callback=delivery_success_callback,
            delivery_error_callback=delivery_error_callback,
            metrics=metrics,
        )

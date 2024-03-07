import logging
import os
import uuid
from pprint import pprint

from kafkian import Consumer
from kafkian.handler import Handler
from kafkian.serde.deserialization import AvroDeserializer

logger = logging.getLogger(__name__)

CONSUMER_CONFIG = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "auto.offset.reset": "earliest",
    "group.id": os.environ.get("CONSUMER_GROUP", str(uuid.uuid4())),
}


def repr_message(message):
    return {
        "topic": message.topic,
        "key": message.key,
        "value": message.value,
        "value_class": message.value.schema.fullname,
    }


topics = ["test"]

print("Initializing consumer for %s" % topics)
consumer = Consumer(
    CONSUMER_CONFIG,
    topics=topics,
    key_deserializer=AvroDeserializer(
        schema_registry_url=os.environ["SCHEMA_REGISTRY_URL"]
    ),
    value_deserializer=AvroDeserializer(
        schema_registry_url=os.environ["SCHEMA_REGISTRY_URL"]
    ),
)

handler = Handler(consumer=consumer)


@handler.handles("test", "*")
def test_topic_fallback(message):
    pprint(repr_message(message))


if __name__ == "__main__":
    handler.run()

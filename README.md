# kafkian 

[![Build Status](https://travis-ci.org/saabeilin/kafkian.svg?branch=master)](https://travis-ci.org/saabeilin/kafkian) [![PyPI](https://img.shields.io/pypi/v/kafkian.svg)](https://pypi.python.org/pypi)

*kafkian* is a opinionated a high-level consumer and producer on top of 
[confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python)/librdkafka 
and partially inspired by [confluent_kafka_helpers](https://github.com/fyndiq/confluent_kafka_helpers). 
It is intended for use primarily in CQRS/EventSourced systems when usage is mostly
limited to producing and consuming encoded messages.

*kafkian* partially mimics Kafka JAVA API, partially is more pythonic, partially just like the maintainer likes it.

Instead of configuring all the things via properties, most of the things 
are planned to be configured explicitely and, wneh possible, via dependency
injection for easier testing. The configuration dictionaries for both producer
and consumer are passed-through directly to underlying confluent producer and 
consumer, hidden behind a facade.

The library provides a base serializer and deserializer classes, as well as 
their specialized Avro subclasses, `AvroSerializer` and `AvroDeserializer`. 
This allows having, say, a plain string key and and avro-encoded message, 
or vice versa. Quite often an avro-encoded string is used as a key, for 
this purpose we provide `AvroStringKeySerializer`.

Unlike the Confluent library, we support supplying the specific Avro schema
together with the message, just like the Kafka JAVA API. Schemas could be
automatically registered with schema registry, also we provide three
`SubjectNameStrategy`, again compatible with Kafka JAVA API.

## Usage
### Producing messages

#### 1. Initialize the producer

```python
from kafkian import Producer
from kafkian.serde.serialization import AvroSerializer, AvroStringKeySerializer, SubjectNameStrategy

producer = Producer(
    {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
    },
    key_serializer=AvroStringKeySerializer(schema_registry_url=config.SCHEMA_REGISTRY_URL),
    value_serializer=AvroSerializer(schema_registry_url=config.SCHEMA_REGISTRY_URL,
                                    value_subject_name_strategy=SubjectNameStrategy.RecordNameStrategy)
)

```

#### 2. Define your message schema(s)

```python
from avro.schema import Schema

class Message(dict):
    _schema: Schema = None


value_schema_str = """
{
   "namespace": "auth.users",
   "name": "UserCreated",
   "type": "record",
   "fields" : [
     {
       "name" : "uuid",
       "type" : "string"
     },     
     {
       "name" : "name",
       "type" : "string"
     },
     {
        "name": "timestamp",
        "type": {
            "type": "long",
            "logicalType": "timestamp-millis"
        }
     }
   ]
}
"""


class UserCreated(Message):
    _schema = avro.loads(value_schema_str)

```

#### 3. Produce the message

```python

producer.produce(
    "auth.users.events",
    user.uuid,
    UserCreated({
        "uuid": user.uuid,
        "name": user.name,
        "timestamp": int(user.timestamp.timestamp() * 1000)
    }),
    sync=True
)
```

### Consuming messages

#### 1. Initialize the consumer

```python
CONSUMER_CONFIG = {
    'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
    'default.topic.config': {
        'auto.offset.reset': 'latest',
    },
    'group.id': 'notifications'
}

consumer = Consumer(
    CONSUMER_CONFIG,
    topics=["auth.users.events"],
    key_deserializer=AvroDeserializer(schema_registry_url=config.SCHEMA_REGISTRY_URL),
    value_deserializer=AvroDeserializer(schema_registry_url=config.SCHEMA_REGISTRY_URL),
)
```

#### 2. Consume the messages via the generator

```python

for message in consumer:
    handle_message(message)
    consumer.commit()
```

Here, `message` is an instance of `Message` class exposed by the 
confluent-kafka-python, access the decoded key and value via `.key()` 
and `.value()` respectively.

Avro schemas for consumed messages will be available as soon as 
[this pull request](https://github.com/confluentinc/confluent-kafka-python/pull/453) 
is completed and merged.

## Contributing
This libarary is, as stated, quite opinionated, however, I'm open to suggestions.
Write your questions and suggestions as issues here on github!

#### Running tests
Both unit and system tests are provided. 

To run unit-tests, install the requirements and just run 
```bash
py.test tests/unit/
``` 

To run system tests, a Kafka cluster together with a schema registry is 
required. A Docker compose file is provided, just run 
```bash
docker-compose up
```
and once the cluster is up and running, run system tests via 
```bash
py.test tests/system/
```


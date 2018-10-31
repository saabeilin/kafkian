import pytest
from confluent_kafka import avro

from kafkian.serde.serialization import SubjectNameStrategy, AvroSerializer

value_schema_str = """
{
   "namespace": "my.test",
   "name": "Value",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

schema = avro.loads(value_schema_str)


@pytest.mark.parametrize(
    'strategy,is_key,subject',
    (
            (SubjectNameStrategy.TopicNameStrategy, False, 'topic-value'),
            (SubjectNameStrategy.TopicNameStrategy, True, 'topic-key'),
            (SubjectNameStrategy.RecordNameStrategy, False, 'my.test.Value'),
            (SubjectNameStrategy.RecordNameStrategy, True, 'my.test.Value'),
            (SubjectNameStrategy.TopicRecordNameStrategy, False, 'topic-my.test.Value'),
            (SubjectNameStrategy.TopicRecordNameStrategy, True, 'topic-my.test.Value'),
    )
)
def test_subject_name_strategy(strategy, is_key, subject):
    ser = AvroSerializer('', subject_name_strategy=strategy)
    assert ser._get_subject("topic", schema, is_key) == subject

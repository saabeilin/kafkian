import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.mock_schema_registry_client import (
    MockSchemaRegistryClient,
)

SCHEMA_REGISTRY_URL = "mock://"


@pytest.fixture(scope="module")
def mock_schema_registry_client() -> SchemaRegistryClient:
    return MockSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

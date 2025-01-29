#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import json
import struct

from confluent_kafka.schema_registry import (
    _MAGIC_BYTE,
    Schema,
)
from confluent_kafka.schema_registry.json_schema import (
    _ContextStringIO,
    _resolve_named_schema,
)
from confluent_kafka.serialization import Deserializer, SerializationError
from jsonschema import RefResolver, ValidationError, validate


class JSONDeserializer(Deserializer):
    """Deserializer for JSON encoded data with Confluent Schema Registry
    framing.

    Args:
    ----
        schema_str (str, Schema):
            `JSON schema definition <https://json-schema.org/understanding-json-schema/reference/generic.html>`_
            Accepts schema as either a string or a :py:class:`Schema` instance.
            Note that string definitions cannot reference other schemas. For referencing other schemas,
            use a :py:class:`Schema` instance.

        from_dict (callable, optional): Callable(dict, SerializationContext) -> object.
            Converts a dict to a Python object instance.

        schema_registry_client (SchemaRegistryClient, optional): Schema Registry client instance. Needed if ``schema_str`` is a schema referencing other schemas.

    """  # noqa: E501

    __slots__ = [
        "_parsed_schema",
        "_from_dict",
        "_registry",
        "_are_references_provided",
        "_schema",
    ]

    def __init__(self, schema_registry_client, schema_str=None, from_dict=None):
        self._are_references_provided = False
        if schema_str is not None:
            if isinstance(schema_str, str):
                schema = Schema(schema_str, schema_type="JSON")
            elif isinstance(schema_str, Schema):
                schema = schema_str
                self._are_references_provided = bool(schema_str.references)
                if self._are_references_provided and schema_registry_client is None:
                    raise ValueError(
                        """schema_registry_client must be provided if "schema_str"""
                        ""
                        """is a Schema instance with references"""
                    )
            else:
                raise TypeError("You must pass either str or Schema")

            self._parsed_schema = json.loads(schema.schema_str)
            self._schema = schema
        else:
            self._parsed_schema = None
            self._schema = None

        self._registry = schema_registry_client

        if from_dict is not None and not callable(from_dict):
            raise ValueError(
                "from_dict must be callable with the signature"
                " from_dict(dict, SerializationContext) -> object"
            )

        self._from_dict = from_dict

    def __call__(self, data, ctx):
        """Deserialize a JSON encoded record with Confluent Schema Registry framing to
        a dict, or object instance according to from_dict if from_dict is specified.

        Args:
        ----
            data (bytes): A JSON serialized record with Confluent Schema Regsitry framing.

            ctx (SerializationContext): Metadata relevant to the serialization operation.

        Returns:
        -------
            A dict, or object instance according to from_dict if from_dict is specified.

        Raises:
        ------
            SerializerError: If there was an error reading the Confluent framing data, or
               if ``data`` was not successfully validated with the configured schema.

        """
        if data is None:
            return None

        if len(data) <= 5:
            raise SerializationError(
                "Expecting data framing of length 6 bytes or "
                f"more but total data size is {len(data)} bytes. This "
                "message was not produced with a Confluent "
                "Schema Registry serializer"
            )

        with _ContextStringIO(data) as payload:
            magic, schema_id = struct.unpack(">bI", payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError(
                    f"Unexpected magic byte {magic}. This message "
                    "was not produced with a Confluent "
                    "Schema Registry serializer"
                )

            # JSON documents are self-describing; no need to query schema
            obj_dict = json.loads(payload.read())

            try:
                if not self._schema:
                    self._schema = self._registry.get_schema(schema_id)
                    if self._schema.schema_type != "JSON":
                        raise SerializationError(
                            f"Schema type mismatch. Expected JSON, got {self._schema.schema_type}"
                        )
                    self._parsed_schema = json.loads(self._schema.schema_str)

                if self._are_references_provided:
                    named_schemas = _resolve_named_schema(self._schema, self._registry)
                    validate(
                        instance=obj_dict,
                        schema=self._parsed_schema,
                        resolver=RefResolver(
                            self._parsed_schema.get("$id"),
                            self._parsed_schema,
                            store=named_schemas,
                        ),
                    )
                else:
                    validate(instance=obj_dict, schema=self._parsed_schema)
            except ValidationError as ve:
                raise SerializationError(ve.message) from ve

            if self._from_dict is not None:
                return self._from_dict(obj_dict, ctx)

            return obj_dict

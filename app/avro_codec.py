from __future__ import annotations

import json
from functools import lru_cache
from io import BytesIO

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from app.settings import load_schema


@lru_cache(maxsize=None)
def parsed_schema(schema_name: str) -> dict:
    return parse_schema(json.loads(load_schema(schema_name)))


def encode_avro(record: dict, schema_name: str) -> bytes:
    buffer = BytesIO()
    schemaless_writer(buffer, parsed_schema(schema_name), record)
    return buffer.getvalue()


def decode_avro(payload: bytes, schema_name: str) -> dict:
    return schemaless_reader(BytesIO(payload), parsed_schema(schema_name))

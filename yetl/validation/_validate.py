import json
from ..resource import get_resource_text
import jsonschema as js
from enum import Enum


class SchemaFiles(Enum):
    tables_schema = "sibytes_yetl_tables_schema.json"
    pipeline_schema = "sibytes_yetl_pipeline_schema.json"
    project_schema = "sibytes_yetl_project_schema.json"


def get_table_schema():
    """Get the tables json schema from the package resource"""
    schema = get_resource_text(SchemaFiles.tables_schema.value)
    json_schema = json.loads(schema)

    return json_schema


def get_pipeline_schema():
    """Get the pipeline json schema from the package resource"""
    schema = get_resource_text(SchemaFiles.pipeline_schema.value)
    json_schema = json.loads(schema)

    return json_schema


def get_project_schema():
    """Get the project json schema from the package resource"""
    schema = get_resource_text(SchemaFiles.project_schema.value)
    json_schema = json.loads(schema)

    return json_schema


def get_schema(schema_file: SchemaFiles):
    if schema_file == SchemaFiles.pipeline_schema:
        return get_pipeline_schema()
    if schema_file == SchemaFiles.project_schema:
        return get_project_schema()
    if schema_file == SchemaFiles.tables_schema:
        return get_table_schema()


def validate_tables(data: dict) -> bool:
    schema = get_table_schema()
    js.validate(instance=data, schema=schema)


def validate_pipeline(data: dict) -> bool:
    schema = get_pipeline_schema()
    js.validate(instance=data, schema=schema)

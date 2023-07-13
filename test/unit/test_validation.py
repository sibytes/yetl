from yetl.validation import _validate as v
import yaml
from jsonschema import SchemaError, ValidationError


def test_get_table_schema():

    schema = v.get_table_schema()

    assert isinstance(schema, dict)
    assert schema is not None

def test_validate_valid_tables():

    with open("./test/config/test_project/pipelines/tables.yaml", "r", encoding="utf-8") as f:
        tables = yaml.safe_load(f)

    try:
        v.validate_tables(tables)
        assert True
    except ValidationError:
        assert False

def test_validate_invalid_tables():

    with open("./test/config/test_project/pipelines/tables_invalid.yaml", "r", encoding="utf-8") as f:
        tables = yaml.safe_load(f)

    try:
        v.validate_tables(tables)
        assert False
    except Exception as e:
        assert isinstance(e, ValidationError)
        assert e.message == "'version' is a required property"


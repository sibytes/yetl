from yetl.flow.dataset import (
    SchemaProperties,
    LineageProperties,
    DeltaWriterProperties,
    ReaderProperties,
)
import json
from unittest import TestCase


schema_properties = {
    "yetl.schema.createIfNotExists": True,
    "yetl.schema.corruptRecord": False,
    "yetl.schema.corruptRecordName": "_corrupt_record",
}

lienage_properties = {
    "yetl.metadata.contextId": True,
    "yetl.metadata.dataflowId": True,
    "yetl.metadata.datasetId": True,
}

reader_properties = (
    lienage_properties
    | schema_properties
    | {
        "yetl.metadata.timeslice": "timeslice_file_date_format",
        "yetl.metadata.filepathFilename": True,
        "yetl.metadata.filepath": True,
        "yetl.metadata.filename": True,
    }
)

delta_writer_properties = (
    lienage_properties
    | schema_properties
    | {
        "yetl.delta.optimizeZOrderBy": True,
    }
)


def test_base_schema_properties():

    props = SchemaProperties(**schema_properties)
    actual: dict = json.loads(props.json())
    expected = schema_properties

    assert expected == actual


def test_base_lienage_properties():

    props = LineageProperties(**lienage_properties)
    actual: dict = json.loads(props.json())
    expected = lienage_properties

    assert expected == actual


def test_reader_properties():

    props = ReaderProperties(**reader_properties)
    actual: dict = json.loads(props.json())
    expected = reader_properties

    assert expected == actual


def test_delta_writer_properties():

    props = DeltaWriterProperties(**delta_writer_properties)
    actual: dict = json.loads(props.json())
    expected = delta_writer_properties

    assert expected == actual

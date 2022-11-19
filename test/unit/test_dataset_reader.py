from yetl.model.dataset import Reader
import json
from collections import OrderedDict
import uuid
from unittest import TestCase

reader_config = {
    "timeslice": {
        "year": "*",
        "month": "*",
        "day": "*",
        "hour": 0,
        "minute": 0,
        "second": 0,
        "microsecond": 0
    },
    "context_id": "bf76fc9b-94d0-4e86-bf80-7e74893dd487",
    "dataflow_id": "26658212-b2b4-4aaa-8173-393829d57bd2",
    "dataframe": None,
    "dataset_id": "decb22d6-1dee-450c-8c20-9b09b6684159",
    "datalake_protocol": "file:",
    "datalake": "c/mylake",
    "database": "customer",
    "table": "landing",
    "properties": {
        "yetl.schema.createIfNotExists": True,
        "yetl.schema.corruptRecordName": "_corrupt_record",
        "yetl.schema.corruptRecord": False,
        "yetl.metadata.contextId": True,
        "yetl.metadata.dataflowId": True,
        "yetl.metadata.datasetId": True,
        "yetl.metadata.timeslice": "timeslice_file_date_format",
        "yetl.metadata.filepathFilename": True,
        "yetl.metadata.filepath": True,
        "yetl.metadata.filename": True
    },
    "path_date_format": "%Y%m%d",
    "file_date_format": "%Y%m%d",
    "format": "csv",
    "path": "landing/demo/{{ timeslice_path_date_format }}/customer_details_{{ timeslice_file_date_format }}.csv",
    "read": {
        "auto": True,
        "options": {
            "mode": "PERMISSIVE",
            "inferSchema": True,
            "header": True
        }
    },
    "exceptions": {
        "path": "delta_lake/demo_landing/{{table_name}}_exceptions",
        "database": "demo_landing",
        "table": "{{table_name}}_exceptions"
    },
    "thresholds": {
        "warning": {
            "min_rows": 1,
            "max_rows": 1000,
            "exception_count": 0,
            "exception_percent": 0
        },
        "error": {
            "min_rows": 0,
            "max_rows": 100000000,
            "exception_count": 50,
            "exception_percent": 80
        }
    }
}


def test_reader():

    reader = Reader.parse_obj(reader_config)
    actual: dict = json.loads(reader.json())
    expected = reader_config
    expected["path"] = "file:c/mylake/landing/demo/***/customer_details_***.csv"

    TestCase().assertDictEqual(expected, actual)

def test_reader_database_table():

    reader = Reader.parse_obj(reader_config)
    database = reader_config["database"]
    table = reader_config["table"]
    expected = f"`{database}`.`{table}`"
    actual = reader.sql_database_table

    assert expected == actual

def test_reader_database_table():

    reader = Reader.parse_obj(reader_config)
    database = reader_config["database"]
    table = reader_config["table"]
    expected = f"{database}.{table}"
    actual = reader.database_table

    assert expected == actual

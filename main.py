# from pipelines import batch_text_csv_to_delta_permissive_1
# from pipelines import batch_text_csv_to_delta_permissive_merge
# from pipelines import batch_sql_to_delta
# from yetl import __main__
# from pipelines import humanresourcesdepartment_landing_to_raw
# from pipelines import adworks_landing_to_raw


# from src import customer_details_landing_to_raw
# from src import customer_preferences_landing_to_raw
# from src import demo_joined_landing_to_raw
# from src import demo_landing_to_raw


from yetl.model._reader import Reader
import json
from collections import OrderedDict
import uuid

reader_config = {
    "context_id": "bf76fc9b-94d0-4e86-bf80-7e74893dd487",
    "database": "customer",
    "dataframe": None,
    "dataflow_id": "26658212-b2b4-4aaa-8173-393829d57bd2",
    "datalake_protocol": "file:",
    "datalake": "c/mylake",
    "dataset_id": "decb22d6-1dee-450c-8c20-9b09b6684159",
    "table": "landing",
    "properties": {
        "yetl.schema.createIfNotExists": True,
        "yetl.schema.corruptRecord": False,
        "yetl.schema.corruptRecordName": "_corrupt_record",
        "yetl.metadata.contextId": True,
        "yetl.metadata.dataflowId": True,
        "yetl.metadata.datasetId": True,
        "yetl.metadata.timeslice": "timeslice_file_date_format",
        "yetl.metadata.filepathFilename": True,
        "yetl.metadata.filepath": True,
        "yetl.metadata.filename": True,
    },
    "path_date_format": "%Y%m%d",
    "file_date_format": "%Y%m%d",
    "format": "csv",
    "path": "landing/demo/{{ timeslice_path_date_format }}/customer_details_{{ timeslice_file_date_format }}.csv",
    "read": {
        "auto": True,
        "options": {"mode": "PERMISSIVE", "inferSchema": False, "header": True},
    },
    "exceptions": {
        "path": "delta_lake/demo_landing/{{table_name}}_exceptions",
        "database": "demo_landing",
        "table": "{{table_name}}_exceptions",
    },
    "thresholds": {
        "warning": {
            "min_rows": 1,
            "max_rows": 1000,
            "exception_count": 0,
            "exception_percent": 0,
        },
        "error": {
            "min_rows": 0,
            "max_rows": 100000000,
            "exception_count": 50,
            "exception_percent": 80,
        },
    },
}


reader = Reader.parse_obj(reader_config)
actual: dict = OrderedDict(json.loads(reader.json()))
expected = OrderedDict(dict(reader_config))

print(json.dumps(actual, indent=4, default=str))


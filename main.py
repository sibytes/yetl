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

reader_config = {
    "type": "Reader",
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
}

reader = Reader.parse_obj(reader_config)
actual: dict = json.loads(reader.json())
expected = reader_config

print(actual)

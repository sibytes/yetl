from yetl.model._reader import Reader
import json
from collections import OrderedDict

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


def test_delta_writer_properties():

    reader = Reader.parse_obj(reader_config)
    actual: dict = OrderedDict(json.loads(reader.json()))
    expected = OrderedDict(dict(reader_config))
    del expected["type"]

    assert expected == actual

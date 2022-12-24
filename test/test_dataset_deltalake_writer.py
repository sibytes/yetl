from yetl.flow.dataset import DeltaWriter
import json
from unittest import TestCase
from .fixtures_config import *


def test_deltalake_writer_sql_database_table(
    deltalake_writer_dataset: DeltaWriter, deltalake_writer_dataset_config: dict
):

    database = deltalake_writer_dataset_config["database"]
    table = deltalake_writer_dataset_config["table"]

    expected = f"`{database}`.`{table}`"
    actual = deltalake_writer_dataset.sql_database_table
    assert expected == actual


def test_reader_database_table(
    deltalake_writer_dataset: DeltaWriter, deltalake_writer_dataset_config: dict
):

    database = deltalake_writer_dataset_config["database"]
    table = deltalake_writer_dataset_config["table"]

    expected = f"{database}.{table}"
    actual = deltalake_writer_dataset.database_table
    assert expected == actual

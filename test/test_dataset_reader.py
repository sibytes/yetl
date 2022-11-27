from yetl.flow.dataset import Reader
import json
from unittest import TestCase
from .fixtures_config import *


def test_reader_sql_database_table(reader_dataset: Reader, reader_dataset_config: dict):

    database = reader_dataset_config["database"]
    table = reader_dataset_config["table"]

    expected = f"`{database}`.`{table}`"
    actual = reader_dataset.sql_database_table
    assert expected == actual


def test_reader_database_table(reader_dataset: Reader, reader_dataset_config: dict):

    database = reader_dataset_config["database"]
    table = reader_dataset_config["table"]

    expected = f"{database}.{table}"
    actual = reader_dataset.database_table
    assert expected == actual

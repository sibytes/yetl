from yetl.flow.dataset import SQLReader
import json
from unittest import TestCase
from .fixtures_config import *


def test_sql_reader_sql_database_table(
    sql_reader_dataset: SQLReader, sql_reader_dataset_config: dict
):

    database = sql_reader_dataset_config["database"]
    table = sql_reader_dataset_config["table"]

    expected = f"`{database}`.`{table}`"
    actual = sql_reader_dataset.sql_database_table
    assert expected == actual


def test_reader_database_table(
    sql_reader_dataset: SQLReader, sql_reader_dataset_config: dict
):

    database = sql_reader_dataset_config["database"]
    table = sql_reader_dataset_config["table"]

    expected = f"{database}.{table}"
    actual = sql_reader_dataset.database_table
    assert expected == actual

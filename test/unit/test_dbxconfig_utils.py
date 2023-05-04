import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    LongType,
)
from dbxconfig import _utils as utils
import re


def remove_white_space(string: str):
    pattern = re.compile(r"\s+")
    string = re.sub(pattern, "", string)
    return string


@pytest.fixture
def spark_schema():
    spark_schema = StructType(
        [
            StructField("firstname", StringType(), True),
            StructField("middlename", StringType(), True),
            StructField("lastname", StringType(), True),
            StructField("id", LongType(), True),
            StructField("gender", StringType(), True),
            StructField("salary", DecimalType(4, 2), True),
            StructField("age", IntegerType(), True),
        ]
    )

    return spark_schema


@pytest.fixture
def replacements():
    replacements = {
        utils.JinjaVariables.DATABASE: "test_database",
        utils.JinjaVariables.TABLE: "test_table",
        utils.JinjaVariables.CHECKPOINT: "test_checkpoint",
        utils.JinjaVariables.FILENAME_DATE_FORMAT: "test_filename_date_format",
        utils.JinjaVariables.PATH_DATE_FORMAT: "test_path_date_format",
        utils.JinjaVariables.CONTAINER: "test_container",
    }
    return replacements


def test_utils_get_dll_header(spark_schema):
    actual = utils.get_ddl(spark_schema=spark_schema, header=True)
    expected = [
        "firstname string",
        "middlename string",
        "lastname string",
        "id bigint",
        "gender string",
        "salary decimal(4,2)",
        "age int",
    ]

    assert actual == expected


def test_utils_get_dll_noheader(spark_schema):
    actual = utils.get_ddl(spark_schema=spark_schema, header=False)
    expected = [
        "_c0 string",
        "_c1 string",
        "_c2 string",
        "_c3 bigint",
        "_c4 string",
        "_c5 decimal(4,2)",
        "_c6 int",
    ]

    assert actual == expected


def test_render_jinja(replacements):
    data = """
        {{database}}
        {{table}}
        {{checkpoint}}
        {{filename_date_format}}
        {{path_date_format}}
        {{container}}
    """

    actual = utils.render_jinja(data, replacements)
    expected = """
        test_database
        test_table
        test_checkpoint
        test_filename_date_format
        test_path_date_format
        test_container
    """

    assert actual == expected


def test_render_jinja_skip():
    data = """
        {{database}}
    """

    replacements = {utils.JinjaVariables.DATABASE: None}

    actual = utils.render_jinja(data, replacements)
    expected = """
        {{database}}
    """

    assert actual == expected


def test_get_html_table():
    test_kv = {"test": "succeeded", "test1": {"test1_1": "also fine"}}
    actual = remove_white_space(utils.get_html_table(test_kv))
    expected = """
    <table>
    <tr><th>Name</th><th>Source</th><th>Destination</th></tr>
    <tr><td>test</td><td>succeeded</td></tr>
    <tr><td>test1.test1_1</td><td>also fine</td></tr>
    </table>
    """
    expected = remove_white_space(expected)

    assert actual == expected

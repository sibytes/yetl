import pytest
import yaml
import os
from yetl.flow.context import SparkContext
from yetl.flow.audit import Audit
from yetl.flow.dataset import Reader, DeltaWriter, SQLReader
from yetl.flow._environment import Environment


def load_config(name: str) -> dict:
    path = os.path.curdir
    path = os.path.abspath(path)
    path = os.path.join(path, "test", "config", name)
    path = f"{path}.yaml"
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


@pytest.fixture
def i_context_config():
    return load_config("i_context")


@pytest.fixture
def spark_context_config():
    return load_config("spark_context")


@pytest.fixture
def databricks_context_config():
    return load_config("databricks_context")


@pytest.fixture
def spark_context(spark_context_config: dict):
    environment = Environment()
    context = SparkContext(
        auditor=Audit(),
        project="demo",
        name="demo",
        environment=environment,
        **spark_context_config,
    )
    return context


@pytest.fixture
def reader_dataset_config():
    return load_config("reader_dataset")


@pytest.fixture
def reader_dataset(reader_dataset_config: dict, spark_context: SparkContext):
    reader = Reader(context=spark_context, **reader_dataset_config)
    return reader


@pytest.fixture
def deltalake_writer_dataset_config():
    return load_config("deltalake_writer_dataset")


@pytest.fixture
def deltalake_writer_dataset(
    deltalake_writer_dataset_config: dict, spark_context: SparkContext
):
    delta_writer = DeltaWriter(context=spark_context, **deltalake_writer_dataset_config)
    return delta_writer


@pytest.fixture
def sql_reader_dataset_config():
    return load_config("sql_reader_dataset")


@pytest.fixture
def sql_reader_dataset(sql_reader_dataset_config: dict, spark_context: SparkContext):
    sql_reader = SQLReader(context=spark_context, **sql_reader_dataset_config)
    return sql_reader

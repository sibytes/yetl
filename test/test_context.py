from yetl.flow.context import IContext, SparkContext, DatabricksContext
from yetl.flow.audit import Audit
from unittest import TestCase
import json
from yetl.flow.file_system import FileSystemType
from .fixtures_config import (
    i_context_config,
    spark_context_config,
    databricks_context_config,
)
from typing import Callable
from yetl.flow._environment import Environment


def test_icontext(i_context_config: Callable):
    class TestContext(IContext):
        def _get_deltalake_flow(self):
            pass

    environment = Environment()
    context = TestContext(
        auditor=Audit(),
        project="demo",
        name="demo",
        environment=environment,
        **i_context_config
    )

    assert context.datalake == i_context_config["datalake"]
    assert context.datalake_protocol == FileSystemType.FILE
    assert (
        context.pipeline_repository.pipeline_root
        == i_context_config["pipeline_repo"]["pipeline_file"]["pipeline_root"]
    )
    assert (
        context.pipeline_repository.sql_root
        == i_context_config["pipeline_repo"]["pipeline_file"]["sql_root"]
    )


def test_spark_context(spark_context_config: Callable):

    environment = Environment()
    context = SparkContext(
        auditor=Audit(),
        project="demo",
        name="demo",
        environment=environment,
        **spark_context_config
    )

    assert context.datalake == spark_context_config["datalake"]
    assert context.datalake_protocol == FileSystemType.FILE
    assert (
        context.pipeline_repository.pipeline_root
        == spark_context_config["pipeline_repo"]["pipeline_file"]["pipeline_root"]
    )
    assert (
        context.pipeline_repository.sql_root
        == spark_context_config["pipeline_repo"]["pipeline_file"]["sql_root"]
    )


def test_databricks_context(databricks_context_config: Callable):

    environment = Environment()
    expected = "Cannot import DBUtils, most likely cause is having DBFS configured for environment that isn't databricks and doesn't support."
    actual: str
    try:
        context = DatabricksContext(
            auditor=Audit(),
            project="demo",
            name="demo",
            environment=environment,
            **databricks_context_config
        )
    except Exception as e:
        actual = str(e)

    assert actual == expected

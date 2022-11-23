from yetl.flow.context import IContext
from yetl.flow.audit import Audit
from unittest import TestCase
import json
from yetl.flow.file_system import FileSystemType
from .fixtures_config import i_context_config
from typing import Callable


def test_icontext(i_context_config: Callable):
    class TestContext(IContext):
        def _get_deltalake_flow(self):
            pass

    context = TestContext(
        auditor=Audit(), project="demo", name="demo", **i_context_config
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

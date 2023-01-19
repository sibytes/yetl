from enum import Enum

from ._pipeline_file_repo import PipelineFileRepo
from ._i_pipeline_repo import IPipelineRepo
import logging


class PipelineRepoType(Enum):
    PIPELINE_FILE = "pipeline_file"


class _PipelineRepoFactory:
    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._pipeline_repo = {}

    def register_pipeline_repo_type(
        self, pr_type: PipelineRepoType, pipeline_repo_type: type
    ):
        self._logger.debug(f"Register pipeline type {pipeline_repo_type} as {type}")
        self._pipeline_repo[pr_type] = pipeline_repo_type

    def get_pipeline_repo_type(self, config: dict) -> IPipelineRepo:

        pipeline_repo_store: str = next(iter(config))
        pr_type: PipelineRepoType = PipelineRepoType(pipeline_repo_store)

        self._logger.debug(f"Setting up pipeline repo on {pipeline_repo_store} ")

        self._logger.debug(f"Setting PipelineRepoType using type {pr_type}")
        pipeline_repo: IPipelineRepo = self._pipeline_repo.get(pr_type)
        pipeline_args = config.get(pipeline_repo_store, {})

        return pipeline_repo(**pipeline_args)


factory = _PipelineRepoFactory()
factory.register_pipeline_repo_type(PipelineRepoType.PIPELINE_FILE, PipelineFileRepo)

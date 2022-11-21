from enum import Enum

from ._pipeline_file_repo import PipelineFileRepo
from ._ipipeline_repo import IPipelineRepo
import logging


class PipelineRepoType(Enum):
    PIPELINE_FILE = "pipeline_file"


class _PipelineRepoFactory:
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)
        self._pipeline_repo = {}

    def register_pipeline_repo_type(
        self, 
        pr_type: PipelineRepoType, 
        pipeline_repo_type: type
    ):
        self._logger.debug(f"Register pipeline type {pipeline_repo_type} as {type}")
        self._pipeline_repo[pr_type] = pipeline_repo_type


    def get_pipeline_repo_type(self, log, config: dict) -> IPipelineRepo:

        pipeline_repo_store: str = next(iter(config))
        pr_type: PipelineRepoType = PipelineRepoType(pipeline_repo_store)

        log.info(f"Setting up pipeline repo on {pipeline_repo_store} ")

        log.debug(f"Setting PipelineRepoType using type {pr_type}")
        pipeline_repo: IPipelineRepo = self._pipeline_repo.get(pr_type)

        if not pipeline_repo:
            self._logger.error(
                f"PipelineRepoType {pr_type.name} not registered in the pipeline_repo factory"
            )
            raise ValueError(pr_type)

        return pipeline_repo(**config)


factory = _PipelineRepoFactory()
factory.register_pipeline_repo_type(PipelineRepoType.PIPELINE_FILE, PipelineFileRepo)

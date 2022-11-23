from pydantic import Field
from ._ipipeline_repo import IPipelineRepo


class PipelineFileRepo(IPipelineRepo):

    pipeline_root: str = Field(...)
    sql_root: str = Field(...)

    def load_pipeline(self, name: str):
        pass

    def load_pipeline_sql(self, name: str):
        pass

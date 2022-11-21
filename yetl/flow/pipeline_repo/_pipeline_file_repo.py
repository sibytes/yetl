from pydantic import Field
from ._ipipeline_repo import IPipelineRepo


class PipelineFileRepo(IPipelineRepo):

    pipeline_root:str = Field(default="./config/{{project}}/pipelines")
    sql_root:str = Field(default="./config/{{project}}/sql")

    def load_pipeline(self, name:str):
        pass

    def load_pipeline_sql(self, name:str):
        pass
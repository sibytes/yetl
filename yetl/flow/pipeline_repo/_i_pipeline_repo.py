from abc import ABC, abstractmethod
from pydantic import BaseModel


class IPipelineRepo(BaseModel, ABC):
    @abstractmethod
    def load_pipeline(self, name: str):
        """Loads a pipeline."""
        pass

    @abstractmethod
    def load_pipeline_sql(self, database: str, table: str, path: str):
        """Loads a pipeline sql component"""
        pass

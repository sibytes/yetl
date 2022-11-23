from abc import ABC, abstractmethod
from pydantic import BaseModel


class IPipelineRepo(BaseModel, ABC):
    @abstractmethod
    def load_pipeline(self, name: str):
        """Loads a pipeline from a file."""
        pass

    @abstractmethod
    def load_pipeline_sql(self, name: str):
        """Loads a pipeline sql component from a file."""
        pass

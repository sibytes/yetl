from abc import ABC, abstractmethod
from pydantic import BaseModel, Field
from ..dataset import Destination


class Save(BaseModel, ABC):
    def __init__(self, dataset:Destination) -> None:
        self.dataset = dataset
    dataset:Destination = Field(...)

    @abstractmethod
    def write(self):
        self.dataset.context.log.info(
            f"Writer saving using the {self.__class__.__name__} "
        )

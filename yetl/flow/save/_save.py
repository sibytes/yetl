from abc import ABC, abstractmethod
from pydantic import BaseModel, Field
from ..dataset import Destination
from typing import Any


class Save(BaseModel, ABC):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    dataset: Destination = Field(default=None)

    @abstractmethod
    def write(self):
        pass
        # self.dataset.context.log.info(
        #     f"Writer saving using the {self.__class__.__name__} "
        # )

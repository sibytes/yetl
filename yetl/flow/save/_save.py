from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, PrivateAttr
from ..dataset import Destination
from typing import Any
import logging


class Save(BaseModel, ABC):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)

    dataset: Destination = Field(default=None)
    _logger: Any = PrivateAttr(default=None)

    @abstractmethod
    def write(self):
        self._logger.debug(f"Writer saving using the {self.__class__.__name__} ")

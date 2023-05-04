from ._dataset import DataSet
import logging
from typing import Any


class Write(DataSet):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._render()

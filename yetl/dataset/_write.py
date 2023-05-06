from ._table import Table
import logging
from typing import Any


class Write(Table):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._render()

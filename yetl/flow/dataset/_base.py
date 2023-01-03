from pydantic import BaseModel
from abc import ABC, abstractmethod, abstractproperty
from pydantic import BaseModel, Field, PrivateAttr
from typing import Any


class Dataset(BaseModel, ABC):

    _logger: Any = PrivateAttr(default=None)

    @abstractmethod
    def initialise(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def verify(self):
        """Validates the dataset, Note: cannot use the verb validate since it clashes with pydantic"""
        pass

    @abstractproperty
    def is_source(self) -> bool:
        pass

    @abstractproperty
    def is_destination(self) -> bool:
        pass

    @abstractproperty
    def initial_load(self) -> bool:
        pass

    def get_metadata(self):
        metadata = {
            str(self.dataset_id): {
                "type": self.__class__.__name__,
                "dataflow_id": str(self.dataflow_id),
                "database": self.database,
                "table": self.table,
            }
        }

        return metadata


class Destination(Dataset, ABC):
    @property
    def is_source(self):
        return False

    @property
    def is_destination(self):
        return True

    @property
    def auto_write(self):

        return self.write.auto

    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):
        self._initial_load = value


class Source(Dataset):
    @property
    def is_source(self):
        return True

    @property
    def is_destination(self):
        return False

    @property
    def auto_read(self):

        return self.read.auto

    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):
        self._initial_load = value


class SQLTable(BaseModel):

    database: str = Field(...)
    table: str = Field(...)

    @property
    def sql_database_table(self, sep: str = ".", qualifier: str = "`") -> str:
        "Concatenated fully qualified database table for SQL"
        return f"{qualifier}{self.database}{qualifier}{sep}{qualifier}{self.table}{qualifier}"

    @property
    def database_table(self, sep: str = ".") -> str:
        "Concatenated database table for readability"
        return f"{self.database}{sep}{self.table}"

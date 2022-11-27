from pydantic import BaseModel
from abc import ABC, abstractmethod, abstractproperty


class Dataset(BaseModel, ABC):
    @abstractmethod
    def initialise(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def validate(self):
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


class Destination(Dataset, ABC):
    @property
    def is_source(self):
        return False

    @property
    def is_destination(self):
        return True

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
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):
        self._initial_load = value

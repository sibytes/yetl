from enum import Enum
from typing import Type
from ._dataset import Dataset
from ._save import Save, DefaultSave, AppendSave
from ._reader import Reader
from ._writer import Writer
from ._stream_reader import StreamReader
from ._stream_writer import StreamWriter
import logging


class IOType(Enum):
    READ = 1
    WRITE = 2
    READSTREAM = 3
    WRITESTREAM = 4


class _DatasetFactory:
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)
        self._dataset = {}

    def register_dataset_type(self, io_type: IOType, dataset_type: type):
        self._logger.debug(f"Register dataset type {dataset_type} as {type}")
        self._dataset[io_type] = dataset_type

    def _get_io_type(self, name: str):
        try:
            if IOType[name.upper()] in IOType:
                return IOType[name.upper()]
        except:
            return None

    def get_dataset_type(
        self,
        context,
        database: str,
        table: str,
        dataset_config: dict,
        save_type: Type[Save] = DefaultSave,
    ) -> Dataset:

        type: IOType = next(
            iter(
                [
                    self._get_io_type(k)
                    for k in dataset_config.keys()
                    if self._get_io_type(k)
                ]
            )
        )

        self._logger.info(f"Get {type.name} from factory dataset")
        dataset_class = self._dataset.get(type)

        # TODO: fix this - need to register save types and lose the if
        if type == IOType.WRITE:

            class InjectDestination(dataset_class, save_type):
                pass

            dataset_class = InjectDestination

        if not dataset_class:
            self._logger.error(
                f"IOType {type.name} not registered in the dataset factory dataset"
            )
            raise ValueError(type)

        return dataset_class(
            context, database, table, dataset_config, type.name.lower()
        )


factory = _DatasetFactory()
factory.register_dataset_type(IOType.READ, Reader)
factory.register_dataset_type(IOType.WRITE, Writer)
factory.register_dataset_type(IOType.READSTREAM, StreamReader)
factory.register_dataset_type(IOType.WRITESTREAM, StreamWriter)

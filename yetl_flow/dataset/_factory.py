from enum import Enum
from typing import Type
from ._dataset import Dataset
from ._reader import Reader
from ._deltalake_writer import DeltaWriter
from ._stream_reader import StreamReader
from ._stream_writer import StreamWriter
import logging
from ..audit import Audit


class IOType(Enum):
    READER = 1
    DELTAWRITER = 2
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
        self, context, database: str, table: str, dataset_config: dict, auditor: Audit
    ) -> Dataset:

        dataset_type:str = dataset_config["type"]
        type: IOType = self._get_io_type(dataset_type)


        self._logger.info(f"Get {type.name} from factory dataset")
        dataset_class = self._dataset.get(type)

        if not dataset_class:
            self._logger.error(
                f"IOType {type.name} not registered in the dataset factory dataset"
            )
            raise ValueError(type)

        return dataset_class(
            context, database, table, dataset_config, type.name.lower(), auditor
        )


factory = _DatasetFactory()
factory.register_dataset_type(IOType.READER, Reader)
factory.register_dataset_type(IOType.DELTAWRITER, DeltaWriter)
factory.register_dataset_type(IOType.READSTREAM, StreamReader)
factory.register_dataset_type(IOType.WRITESTREAM, StreamWriter)

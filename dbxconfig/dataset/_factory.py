from ._deltalake import DeltaLake
from ._read import Read
from ._write import Write
from ._dataset import DataSet, Table
from ._deltalake import DeltaLakeTable
from ._read import ReadTable
import logging
from .dataset_type import DataSetType, TableType
from .._timeslice import Timeslice
from typing import Union


class _DatasetFactory:
    _TIMESLICE = "timeslice"
    _CONFIG_PATH = "config_path"

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._dataset = {}
        self._table = {}

    def register_dataset_type(self, io_type: DataSetType, dataset_type: type):
        self._logger.debug(f"Register dataset type {dataset_type} as {type}")
        self._dataset[io_type] = dataset_type

    def register_table_type(self, io_type: TableType, table_type: type):
        self._logger.debug(f"Register table type {table_type} as {type}")
        self._table[io_type] = table_type

    def _get_dataset_type(
        self,
        table_type: TableType,
        dataset_config: dict,
    ) -> DataSet:
        self._logger.debug(f"Get DataSetType {table_type.value} from dataset factory")

        dataset_type: DataSetType = DataSetType(table_type.value)
        dataset_class = self._dataset.get(dataset_type)

        if not dataset_class:
            self._logger.debug(
                f"DataSetType {dataset_type.name} not registered in the dataset factory dataset"
            )
            raise ValueError(dataset_type)

        return dataset_class(
            **dataset_config,
        )

    def get_data_set(
        self, config: dict, dataset: Union[Table, dict], timeslice: Timeslice
    ):
        if isinstance(dataset, dict):
            for name, table_obj in dataset.items():
                stage_config = self._get_stage_table_config(
                    config, table_obj, timeslice
                )
                dataset[name] = self._get_dataset_type(
                    table_obj.table_type, stage_config
                )

        elif isinstance(dataset, Table):
            stage_config = self._get_stage_table_config(config, dataset, timeslice)
            dataset = self._get_dataset_type(dataset.table_type, stage_config)

        return dataset

    def get_table(self, table_type: TableType, table_config: dict):
        self._logger.debug(f"Get {table_type.name} from factory dataset")
        table_class = self._table.get(table_type)

        if not table_class:
            self._logger.debug(
                f"DataSetType {table_type.name} not registered in the dataset factory dataset"
            )
            raise ValueError(table_type)

        return table_class(
            **table_config,
        )

    def _get_stage_table_config(self, config: dict, table: Table, timeslice: Timeslice):
        """
        injects the table and ancillary attributes into the configuration
        for the typed data set object.
        """
        stage_config = config[table.stage.name][table.table_type.value]
        try:
            stage_config = config[table.stage.name][table.table_type.value]
        except Exception:
            raise Exception(
                f"""Table stage and type not found in configuration.
                Table: {table.stage.name}.{table.table_type.value}
                not int config:
                {config}."""
            )
        stage_config[self._TIMESLICE] = timeslice
        stage_config[self._CONFIG_PATH] = config[self._CONFIG_PATH]
        table_config = table.dict()
        stage_config = {**stage_config, **table_config}

        return stage_config


factory = _DatasetFactory()
factory.register_dataset_type(DataSetType.Read, Read)
factory.register_dataset_type(DataSetType.DeltaLake, DeltaLake)
factory.register_dataset_type(DataSetType.Write, Write)

factory.register_table_type(TableType.ReadTable, ReadTable)
factory.register_table_type(TableType.DeltaLakeTable, DeltaLakeTable)

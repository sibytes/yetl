from enum import Enum
from abc import ABC
from pyspark.sql import DataFrame
from delta import DeltaTable
from ..dataset import Dataset
from ._save import Save

from ._save_mode_type import SaveModeType


class ErrorIfExistsSave(Save):

    def __init__(self, dataset: Dataset) -> None:
        super().__init__(dataset)

    def write(self):
        super().write()
        (
            self.dataset.dataframe.write.format(self.dataset.format)
            .options(**self.dataset.options)
            .mode(SaveModeType.ERROR_IF_EXISTS.value)
            .partitionBy(*self.dataset.partitions)
            .save(self.dataset.path)
        )


class AppendSave(Save):

    def __init__(self, dataset: Dataset) -> None:
        super().__init__(dataset)

    def write(self):
        super().write()
        (
            self.dataset.dataframe.write.format(self.dataset.format)
            .options(**self.dataset.options)
            .mode(SaveModeType.APPEND.value)
            .partitionBy(*self.dataset.partitions)
            .save(self.dataset.path)
        )


class OverwriteSchemaSave(Save):

    def __init__(self, dataset: Dataset) -> None:
        super().__init__(dataset)

    def write(self):
        super().write()
        options = self.dataset.options
        options[SaveModeType.OVERWRITE_SCHEMA.value] = True
        (
            self.dataset.dataframe.write.format(self.dataset.format)
            .options(**options)
            .mode(SaveModeType.OVERWRITE.value)
            .partitionBy(*self.dataset.partitions)
            .save(self.dataset.path)
        )


class OverwriteSave(Save):

    def __init__(self, dataset: Dataset) -> None:
        super().__init__(dataset)

    def write(self):
        super().write()
        (
            self.dataset.dataframe.write.format(self.dataset.format)
            .options(**self.dataset.options)
            .mode(SaveModeType.OVERWRITE.value)
            .partitionBy(*self.dataset.partitions)
            .save(self.dataset.path)
        )


class IgnoreSave(Save):

    def __init__(self, dataset: Dataset) -> None:
        super().__init__(dataset)

    def write(self):
        super().write()
        (
            self.dataset.dataframe.write.format(self.dataset.format)
            .options(**self.dataset.options)
            .mode(SaveModeType.IGNORE.value)
            .partitionBy(*self.dataset.partitions)
            .save(self.dataset.path)
        )


class MergeSave(Save):

    def __init__(self, dataset: Dataset) -> None:
        super().__init__(dataset)

    def write(self):
        super().write()

        tbl = DeltaTable.forPath(self.dataset.context.spark, self.dataset.path)
        (
            tbl.alias('dst').merge(
                self.dataset.dataframe.alias('src'),
                'dst.id = src.id'
            ) 
            .whenMatchedUpdateAll()
            .whenMatchedDelete()
            .whenNotMatchedInsertAll()
            .execute()
        )


class DefaultSave(AppendSave):

    def __init__(self, dataset: Dataset) -> None:
        super().__init__(dataset)

    def write(self):
        super().write()


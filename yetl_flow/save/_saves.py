from enum import Enum
from abc import ABC
from pyspark.sql import DataFrame
from delta import DeltaTable
from ..dataset import Dataset
from ._save import Save
from typing import Union
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

        merge_update_match = self._derive_any_except_match(
            self.dataset.merge_update_match, self.dataset.dataframe
        )
        merge_insert_match = self._derive_any_except_match(
            self.dataset.merge_insert_match, self.dataset.dataframe
        )

        tbl = DeltaTable.forPath(self.dataset.context.spark, self.dataset.path)
        merger = tbl.alias("dst").merge(
            self.dataset.dataframe.alias("src"), self.dataset.merge_join
        )

        if self.dataset.merge_update:
            if merge_update_match:
                merger = merger.whenMatchedUpdateAll(merge_update_match)
            else:
                merger = merger.whenMatchedUpdateAll()

        if self.dataset.merge_insert:
            if merge_insert_match:
                merger = merger.whenNotMatchedInsertAll(merge_insert_match)
            else:
                merger = merger.whenNotMatchedInsertAll()

        if self.dataset.merge_delete:
            if self.dataset.merge_delete_match:
                merger = merger.whenMatchedDelete(self.dataset.merge_delete_match)

        merger.execute()

    def _derive_any_except_match(self, merge_match: Union[str, dict], df: DataFrame):

        if isinstance(merge_match, dict):
            any_except = merge_match.get("any_not_equal_except")
            derived_merge_match = [
                f"src.{c} != dst.{c}"
                for c in df.columns
                if c not in any_except and not c.startswith("_")
            ]
            derived_merge_match = " or ".join(derived_merge_match)
            return derived_merge_match
        else:
            return merge_match


class DefaultSave(AppendSave):
    def __init__(self, dataset: Dataset) -> None:
        super().__init__(dataset)

    def write(self):
        super().write()

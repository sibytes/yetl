from enum import Enum
from abc import ABC
from pyspark.sql import DataFrame


class SaveMode(Enum):
    # stop black linting from breaking the code.
    # fmt: off
    "https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes"
    DEFAULT = "default"
    ERROR_IF_EXISTS = "errorifexists"
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    MERGE = "merge"
    OVERWRITE_SCHEMA = "overwriteSchema"
    # fmt: on


class Save(ABC):
    def __init__(self) -> None:
        pass

    def write(self):
        pass


class DefaultSave(Save):
    def write(self):
        self.context.log.info(
            f"Writer saving using the DefaultSave which is the configured save = {self.mode}."
        )
        (
            self.dataframe.write.format(self.format)
            .options(**self.options)
            .mode(self.mode)
            .partitionBy(*self.partitions)
            .save(self.path)
        )


class ErrorIfExistsSave(Save):
    def write(self):
        self.context.log.info(
            "Writer saving using the ErrorIfExistsSave which is an injected save."
        )
        (
            self.dataframe.write.format(self.format)
            .options(**self.options)
            .mode(SaveMode.ERROR_IF_EXISTS.value)
            .partitionBy(*self.partitions)
            .save(self.path)
        )


class AppendSave(Save):
    def write(self):
        self.context.log.info(
            "Writer saving using the AppendSave which is an injected save."
        )
        (
            self.dataframe.write.format(self.format)
            .options(**self.options)
            .mode(SaveMode.APPEND.value)
            .partitionBy(*self.partitions)
            .save(self.path)
        )


class OverwriteSchemaSave(Save):
    # TODO: overwrites partitions, extend to include partitioning.
    def write(self):
        self.context.log.info(
            "Writer saving using the OverwriteSchemaSave which is an injected save."
        )
        options = self.options
        options[SaveMode.OVERWRITE_SCHEMA.value] = True
        (
            self.dataframe.write.format(self.format)
            .options(**options)
            .mode(SaveMode.OVERWRITE.value)
            .partitionBy(*self.partitions)
            .save(self.path)
        )


class OverwriteSave(Save):
    def write(self):
        self.context.log.info(
            "Writer saving using the OverwriteSave which is an injected save."
        )
        (
            self.dataframe.write.format(self.format)
            .options(**self.options)
            .mode(SaveMode.OVERWRITE.value)
            .partitionBy(*self.partitions)
            .save(self.path)
        )


class IgnoreSave(Save):
    def write(self):
        self.context.log.info(
            "Writer saving using the IgnoreSave which is an injected save."
        )
        (
            self.dataframe.write.format(self.format)
            .options(**self.options)
            .mode(SaveMode.IGNORE.value)
            .partitionBy(*self.partitions)
            .save(self.path)
        )


class MergeSave(Save):
    def write(self):
        self.context.log.info(
            "Writer saving using the MergeSave which is an injected save."
        )
        raise NotImplementedError()

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as fn
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from typing import Callable, Any
from ..parser._constants import *
from pydantic import BaseModel, Field, PrivateAttr
from ..context import IContext
from abc import ABC, abstractmethod
import logging
import json


class ThresholdLimit(BaseModel):
    min_rows: int = Field(default=0)
    max_rows: int = Field(default=None)
    exception_count: int = Field(default=0)
    exception_percent: int = Field(default=0)


class Thresholds(BaseModel):
    warning: ThresholdLimit = Field(default=ThresholdLimit())
    error: ThresholdLimit = Field(default=ThresholdLimit())


class ThresholdLevels(Enum):
    WARNING = "warning"
    ERROR = "error"
    INFO = "info"


class IValidator(BaseModel, ABC):

    dataframe: DataFrame = None
    validation_handler: Callable[[DataFrame], int] = Field(default=None)
    exceptions_count: int = Field(default=0)
    valid_count: int = Field(default=0)
    total_count: int = Field(default=0)
    exception_percent: int = Field(default=0)
    database: str = Field(default=None)
    table: str = Field(default=None)
    warning_thresholds: ThresholdLimit = Field(default=None)
    error_thresholds: ThresholdLimit = Field(default=None)
    level: ThresholdLevels = Field(default=ThresholdLevels.INFO)
    _logger: Any = PrivateAttr(default=None)

    @abstractmethod
    def validate(self) -> dict:
        pass

    def raise_thresholds(self, thresholds: ThresholdLimit, level: ThresholdLevels):

        min_rows = thresholds.min_rows
        max_rows = thresholds.max_rows
        exception_count = thresholds.exception_count
        exception_percent = thresholds.exception_percent

        exception_count = self.total_count - self.valid_count
        self.exception_percent = (exception_count / self.total_count) * 100

        raise_thresholds = False
        messages = []

        if self.total_count <= min_rows:
            raise_thresholds = True
            messages.append(
                f"min_rows threshold exceeded: {self.total_count} < {min_rows}"
            )

        if self.total_count > max_rows:
            raise_thresholds = True
            messages.append(
                f"max_rows threshold exceeded: {self.total_count} > {max_rows}"
            )

        if self.exceptions_count > exception_count:
            raise_thresholds = True
            messages.append(
                f"exception_count threshold exceeded: {self.exceptions_count} >= {exception_count}"
            )

        if self.exception_percent > exception_percent:
            raise_thresholds = True
            messages.append(
                f"exception_percent threshold exceeded: {self.exception_percent} > {exception_percent}"
            )

        if raise_thresholds:
            msg = f"{level.value} Thresholds:\n\t"
            messages = "\n\t".join(messages)
            msg = f"{msg}{messages}"

            if level == ThresholdLevels.ERROR:
                self._logger.error(msg)
                self.level = ThresholdLevels.ERROR

            if level == ThresholdLevels.WARNING:
                self._logger.warning(msg)
                self.level = ThresholdLevels.WARNING

            if level == ThresholdLevels.INFO:
                self._logger.debug(msg)
                self.level = ThresholdLevels.INFO

    def get_result(self):
        validation = {
            "validation": {
                "thresholds": {
                    "warning": self.warning_thresholds.dict(),
                    "error": self.error_thresholds.dict(),
                },
                "schema_on_read": {
                    f"{self.database}.{self.table}": {
                        "total_count": self.total_count,
                        "valid_count": self.valid_count,
                        "exception_count": self.exceptions_count,
                        "exception_percent": self.exception_percent,
                    }
                },
            }
        }
        validation_json = json.dumps(validation, indent=4, default=str)
        if self.level == ThresholdLevels.INFO:
            self._logger.info(validation_json)
        elif ThresholdLevels.WARNING:
            self._logger.warning(validation_json)
        elif ThresholdLevels.ERROR:
            self._logger.error(validation_json)

        return self.level, validation

    class Config:
        arbitrary_types_allowed = True


class PermissiveSchemaOnRead(IValidator):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)

    context: IContext = Field(...)
    corrupt_record: str = Field(default=CORRUPT_RECORD)

    def validate(self):
        self.total_count = self.dataframe.count()
        self.dataframe.cache()

        exceptions = (
            self.dataframe.where(f"{self.corrupt_record} IS NOT NULL")
            .withColumn(TIMESTAMP, fn.current_timestamp())
            .withColumn(DATABASE, fn.lit(self.database))
            .withColumn(TABLE, fn.lit(self.table))
        )

        self.exceptions_count = exceptions.count()

        self.dataframe = self.dataframe.where(f"{self.corrupt_record} IS NULL").drop(
            self.corrupt_record
        )

        self.valid_count = self.dataframe.count()
        self.exceptions_count = self.validation_handler(exceptions)
        if self.warning_thresholds:
            super().raise_thresholds(self.warning_thresholds, ThresholdLevels.WARNING)
        if self.error_thresholds:
            super().raise_thresholds(self.error_thresholds, ThresholdLevels.ERROR)

        return self.get_result()


class BadRecordsPathSchemaOnRead(IValidator):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    context: IContext = Field(...)
    path: str = Field(...)
    spark: SparkSession = None

    def validate(self):

        self.total_count = self.dataframe.count()
        self.dataframe.cache()
        self.valid_count = self.dataframe.distinct().count()
        self.exceptions_count = self.total_count - self.valid_count
        options = {INFER_SCHEMA: True, RECURSIVE_FILE_LOOKUP: True}
        try:
            self._logger.debug(
                f"{self.exceptions_count} schema on read exceptions found for dataset {self.table}"
            )
            if self.exceptions_count > 0:
                self._logger.debug(
                    f"Try loading {self.exceptions_count} exceptions for dataset {self.table} from {self.path}"
                )
                exceptions: DataFrame = (
                    self.spark.read.format("json")
                    .options(**options)
                    .load(self.path)
                    .withColumn(TIMESTAMP, fn.current_timestamp())
                    .withColumn(DATABASE, fn.lit(self.database))
                    .withColumn(TABLE, fn.lit(self.table))
                )
                self.exceptions_count = self.validation_handler(exceptions)
                self._logger.debug(
                    f"Deleting exceptions for dataset {self.table} from {self.path}"
                )
                self.context.fs.rm(self.path, True)

        except AnalysisException as e:
            if self.exceptions_count > 0:
                msg = f"There are {self.exceptions_count} exceptions but dataset for table {self.table} failed to load from path {self.table}"
                self._logger.error(msg)
                raise Exception(msg) from e
            exceptions = None
            self.exceptions_count = 0

        super().raise_thresholds(self.warning_thresholds, ThresholdLevels.WARNING)
        super().raise_thresholds(self.error_thresholds, ThresholdLevels.ERROR)

        return self.get_result()

    class Config:
        arbitrary_types_allowed = True

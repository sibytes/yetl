import numbers
from unicodedata import numeric
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as fn
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from typing import Callable
from ..parser._constants import *
import json


class ThresholdLevels(Enum):
    WARNING = "warning"
    ERROR = "error"
    INFO = "info"


class IValidator:
    def __init__(
        self,
        context,
        dataframe: DataFrame,
        exceptions_handler: Callable[[DataFrame], int],
        database: str,
        table: str,
        warning_thresholds: dict = None,
        error_thresholds: dict = None,
    ) -> None:
        self.context = context
        self.dataframe = dataframe
        self.exceptions_handler = exceptions_handler
        self.exceptions = None
        self.exceptions_count = 0
        self.valid_count = 0
        self.total_count = 0
        self.database = database
        self.table = table
        self.warning_thresholds = warning_thresholds
        self.error_thresholds = error_thresholds
        self.level = ThresholdLevels.INFO

    def validate(self) -> dict:
        pass

    def raise_thresholds(self, thresholds: dict, level: ThresholdLevels):

        if isinstance(thresholds, dict):
            min_rows = thresholds.get("min_rows")
            max_rows = thresholds.get("max_rows")
            exception_count = thresholds.get("exception_count")
            exception_percent = thresholds.get("exception_percent")

            exception_count = self.total_count - self.valid_count
            self.exception_percent = (exception_count / self.total_count) * 100

            raise_thresholds = False
            messages = []
            if isinstance(min_rows, int):
                if self.total_count <= min_rows:
                    raise_thresholds = True
                    messages.append(
                        f"min_rows threshold exceeded: {self.total_count} < {min_rows}"
                    )

            if isinstance(max_rows, int):
                if self.total_count > max_rows:
                    raise_thresholds = True
                    messages.append(
                        f"max_rows threshold exceeded: {self.total_count} > {max_rows}"
                    )

            if isinstance(exception_count, int):
                if self.exceptions_count > exception_count:
                    raise_thresholds = True
                    messages.append(
                        f"exception_count threshold exceeded: {self.exceptions_count} >= {exception_count}"
                    )

            if isinstance(exception_percent, numbers.Number):

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
                    self.context.log.error(msg)
                    self.level = ThresholdLevels.ERROR

                if level == ThresholdLevels.WARNING:
                    self.context.log.warning(msg)
                    self.level = ThresholdLevels.WARNING

                if level == ThresholdLevels.INFO:
                    self.context.log.info(msg)
                    self.level = ThresholdLevels.INFO

    def get_result(self):
        validation = {
            "validation": {
                "thresholds": {
                    "warning": self.warning_thresholds,
                    "error": self.error_thresholds,
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
            self.context.log.info(validation_json)
        elif ThresholdLevels.WARNING:
            self.context.log.warning(validation_json)
        elif ThresholdLevels.ERROR:
            self.context.log.error(validation_json)

        # Python 10
        # match self.level:
        #     case ThresholdLevels.INFO:
        #         self.context.log.info(validation_json)
        #     case ThresholdLevels.WARNING:
        #         self.context.log.warning(validation_json)
        #     case ThresholdLevels.ERROR:
        #         self.context.log.error(validation_json)

        return self.level, validation


class PermissiveSchemaOnRead(IValidator):
    def __init__(
        self,
        context,
        dataframe: DataFrame,
        exceptions_handler: Callable[[DataFrame], int],
        database: str,
        table: str,
        warning_thresholds: dict = None,
        error_thresholds: dict = None,
    ) -> None:
        super().__init__(
            context,
            dataframe,
            exceptions_handler,
            database,
            table,
            warning_thresholds,
            error_thresholds,
        )
        self.database = database
        self.table = table

    def validate(self):

        self.total_count = self.dataframe.count()
        self.dataframe.cache()

        self.exceptions = (
            self.dataframe.where(f"{CORRUPT_RECORD} IS NOT NULL")
            .withColumn(TIMESTAMP, fn.current_timestamp())
            .withColumn(DATABASE, fn.lit(self.database))
            .withColumn(TABLE, fn.lit(self.table))
        )

        self.exceptions_count = self.exceptions.count()
        self.dataframe = self.dataframe.where(f"{CORRUPT_RECORD} IS NULL").drop(
            CORRUPT_RECORD
        )

        self.valid_count = self.dataframe.count()
        self.exceptions_count = self.exceptions_handler(self.exceptions)

        super().raise_thresholds(self.warning_thresholds, ThresholdLevels.WARNING)
        super().raise_thresholds(self.error_thresholds, ThresholdLevels.ERROR)

        return self.get_result()


class BadRecordsPathSchemaOnRead(IValidator):
    def __init__(
        self,
        context,
        dataframe: DataFrame,
        exceptions_handler: Callable[[DataFrame], int],
        database: str,
        table: str,
        bad_records_path: str,
        spark: SparkSession,
        warning_thresholds: dict = None,
        error_thresholds: dict = None,
    ) -> None:
        super().__init__(
            context,
            dataframe,
            exceptions_handler,
            database,
            table,
            warning_thresholds,
            error_thresholds,
        )
        self.path = bad_records_path
        self.spark = spark

    def validate(self):

        self.total_count = self.dataframe.count()
        self.dataframe.cache()
        self.valid_count = self.dataframe.distinct().count()
        self.exceptions_count = self.total_count - self.valid_count
        options = {INFER_SCHEMA: True, RECURSIVE_FILE_LOOKUP: True}
        try:
            self.context.log.info(
                f"{self.exceptions_count} schema on read exceptions found for dataset {self.table}"
            )
            if self.exceptions_count > 0:
                self.context.log.info(
                    f"Try loading {self.exceptions_count} exceptions for dataset {self.table} from {self.path}"
                )
                exceptions = (
                    self.spark.read.format(Format.JSON.name.lower())
                    .options(**options)
                    .load(self.path)
                    .withColumn(TIMESTAMP, fn.current_timestamp())
                    .withColumn(DATABASE, fn.lit(self.database))
                    .withColumn(TABLE, fn.lit(self.table))
                )
                self.exceptions_count = self.exceptions_handler(exceptions)
                self.context.log.info(
                    f"Deleting exceptions for dataset {self.table} from {self.path}"
                )
                self.context.fs.rm(self.path, True)

        except AnalysisException as e:
            if self.exceptions_count > 0:
                msg = f"There are {self.exceptions_count} exceptions but dataset for table {self.table} failed to load from path {self.table}"
                self.context.log.error(msg)
                raise Exception(msg) from e
            exceptions = None
            self.exceptions_count = 0

        super().raise_thresholds(self.warning_thresholds, ThresholdLevels.WARNING)
        super().raise_thresholds(self.error_thresholds, ThresholdLevels.ERROR)

        return self.get_result()

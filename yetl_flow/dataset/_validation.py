from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as fn
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from typing import Callable
from ..parser._constants import *


class IValidator:
    def __init__(
        self,
        context,
        dataframe: DataFrame,
        exceptions_handler: Callable[[DataFrame], int],
        database: str,
        table: str,
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

    def validate(self) -> dict:
        pass

    def get_result(self):
        validation = {
            "validation": {
                "schema_on_read": {
                    f"{self.database}.{self.table}": {
                        "total_count": self.total_count,
                        "valid_count": self.valid_count,
                        "exception_count": self.exceptions_count,
                    }
                }
            }
        }
        return validation


class PermissiveSchemaOnRead(IValidator):
    def __init__(
        self,
        context,
        dataframe: DataFrame,
        exceptions_handler: Callable[[DataFrame], int],
        database: str,
        table: str,
    ) -> None:
        super().__init__(context, dataframe, exceptions_handler, database, table)
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
    ) -> None:
        super().__init__(context, dataframe, exceptions_handler, database, table)
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

        return self.get_result()

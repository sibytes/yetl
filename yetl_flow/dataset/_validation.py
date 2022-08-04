from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as fn
from pyspark.sql import SparkSession
from typing import Callable
from ._constants import Format, CORRUPT_RECORD, DATABASE, TABLE


class IValidator:
    def __init__(
        self,
        dataframe: DataFrame,
        exceptions_handler: Callable[[DataFrame], int],
        database: str,
        table: str,
    ) -> None:
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
        dataframe: DataFrame,
        exceptions_handler: Callable[[DataFrame], int],
        database: str,
        table: str,
    ) -> None:
        super().__init__(dataframe, exceptions_handler, database, table)
        self.database = database
        self.table = table

    def validate(self):

        self.total_count = self.dataframe.count()
        self.dataframe.cache()

        self.exceptions = (
            self.dataframe.where(f"{CORRUPT_RECORD} IS NOT NULL")
            .withColumn("timestamp", fn.current_timestamp())
            .withColumn(DATABASE, fn.lit(self.database))
            .withColumn(TABLE, fn.lit(self.table))
        )

        self.exceptions_count = self.exceptions.count()
        self.dataframe = self.dataframe.where(f"{CORRUPT_RECORD} IS NULL")

        self.valid_count = self.dataframe.count()
        self.exceptions_count = self.exceptions_handler(self.exceptions)

        return self.get_result()


class BadRecordsPathSchemaOnRead(IValidator):
    def __init__(
        self,
        dataframe: DataFrame,
        exceptions_handler: Callable[[DataFrame], int],
        database: str,
        table: str,
        bad_records_path: str,
        spark: SparkSession,
    ) -> None:
        super().__init__(dataframe, exceptions_handler, database, table)
        self.path = bad_records_path
        self.spark = spark

    def validate(self):

        self.total_count = self.dataframe.count()
        self.dataframe.cache()
        self.valid_count = self.dataframe.distinct().count()
        options = {
            "inferSchema": True,
            "recursiveFileLookup": True
        }
        try:
            exceptions = (
                self.spark
                .read
                .format(Format.Json.value)
                .options(**options)
                .load(self.path)
                .withColumn("timestamp", fn.current_timestamp())
                .withColumn(DATABASE, fn.lit(self.database))
                .withColumn(TABLE, fn.lit(self.table))
            )
            display(exceptions)
            self.exceptions_count = exceptions.count()
            self.exceptions_count = self.exceptions_handler(self.exceptions)
        except Exception as e:
            if self.total_count != self.valid_count:
                raise Exception(f"Failed to read exception records at path {self.path}") from e
            exceptions = None
            self.exceptions_count = 0

        return self.get_result()

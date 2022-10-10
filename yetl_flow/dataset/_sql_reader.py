from ._base import Source
from ._dataset import Dataset
from pyspark.sql import functions as fn
from ..parser._constants import *
from . import _builtin_functions as builtin_funcs
from ..schema_repo import ISchemaRepo, SchemaNotFound
from pyspark.sql import DataFrame
import json
from .. import _delta_lake as dl
from ..audit import Audit
import os


class SQLReader(Dataset, Source):
    def __init__(
        self,
        context,
        database: str,
        table: str,
        config: dict,
        io_type: str,
        auditor: Audit,
    ) -> None:
        super().__init__(context, database, table, config, io_type, auditor)

        self.dataframe: DataFrame = None
        # try and load the sql definition
        self.sql: str = self._get_select_sql(config)

        self.context.log.debug(f"SQLReader sql = {self.sql}")

    def _get_select_sql(self, config: dict):

        table = config.get(TABLE)
        if table:
            sql: str = table.get("sql")
            name = table.get("name", self.table)
            schema = table.get("schema", self.database)
            if sql and not "\n" in sql:
                self.schema_repo: ISchemaRepo = (
                    self.context.schema_repo_factory.get_schema_repo_type(
                        self.context, config["deltalake_schema_repo"]
                    )
                )


                sql = self.schema_repo.load_schema(schema, name, sql)
                sql = sql.replace("{{database_name}}", self.database)
                sql = sql.replace("{{table_name}}", self.table)

        else:
            sql = None

        return sql


    def _validate_configuration(self):
        pass

    def _get_table_properties(self, table_config: dict):
        return table_config.get(PROPERTIES, {})

    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):

        self._initial_load = value

    def _get_schema(self, config: dict):

        self.schema_repo: ISchemaRepo = (
            self.context.schema_repo_factory.get_schema_repo_type(self.context, config)
        )
        schema = self.schema_repo.load_schema(self.database, self.table)
        return schema

    def validate(self):

        pass

    def read(self):
        self.context.log.info(
            f"Reading data for {self.database_table} from {self.path} with options {self.options} {CONTEXT_ID}={str(self.context_id)}"
        )

        self.context.log.debug(json.dumps(self.options, indent=4, default=str))

        df: DataFrame = self.context.spark.sql(self.sql)

        self.dataframe = df
        self.validation_result = self.validate()
        return self.dataframe

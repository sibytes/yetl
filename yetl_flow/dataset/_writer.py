from ._destination import Destination
from ._constants import *
from ..schema_repo import ISchemaRepo
import os
from .. import _delta_lake as dl
from pyspark.sql import DataFrame
from typing import ChainMap


class Writer(Destination):
    def __init__(
        self, context, database: str, table: str, config: dict, io_type: str
    ) -> None:
        super().__init__(context, database, table, config, io_type)

        self.dataframe: DataFrame = None
        # try and load a schema if schema on read
        self.table_ddl:str = self._get_table_sql(config)
        self.context.log.debug(f"Writer table ddl = {self.table_ddl}")

        # get the configured partitions.
        self.partitions:list = self._get_partitions(config, self.table_ddl)

        self.auto_optimize = self._get_auto_optimize(config)

        # gets the read, write, etc options based on the type
        io_properties = config.get(io_type)
        self.auto_io = io_properties.get(AUTO_IO, True)
        self.options: dict = io_properties.get(OPTIONS)
        try:
            self.mode = io_properties[MODE]
        except KeyError as e:
            msg = f"{MODE} is missing from the Destination Write configuration and is required."
            context.log.error(msg)
            raise Exception(msg) from e

        self._initial_load = super().initial_load

        if self.auto_io:
            self.context.log.info(
                f"auto_io = {self.auto_io} automatically creating or altering delta table {self.database}.{self.table}"
            )

            # TODO: return the partitions in the table properties
            properties = self.create_or_alter_table()

            self._set_table_constraints(properties, config)
            self._set_table_properties(properties, config)

            # TODO: if the table partitions in the properties form the table is different to the partitions
            # in the config or ddl, yetl.allowRepartitioning = true then repartition the table
            self._table_repartition(properties, config)

    def _table_repartition(self, table_properties:dict, config:dict):
        pass

    def _get_partitions(self, config:dict, table_ddl:str):

        table:dict = config[TABLE]
        partitions = []
        # TODO: change this to a more reliable parser.
        parsed_table_ddl = table_ddl
        if parsed_table_ddl:
            parsed_table_ddl = table_ddl.replace(" ", "").upper()

        if parsed_table_ddl and "PARTITIONEDBY(" in parsed_table_ddl:
            # TODO: get the columns in the PARTITIONED BY clause
            partitions = ["partition_key"]
        else:
            partitions = table.get("partitioned_by")

        return partitions


    def _set_table_constraints(self, table_properties: dict, config: dict):
        _existing_constraints = {}
        if table_properties:
            _existing_constraints = table_properties.get(self.database_table)
            _existing_constraints = _existing_constraints.get("constraints")

        self.column_constraints_ddl = self._get_check_constraints_sql(
            config, _existing_constraints
        )
        self.context.log.debug(
            f"Writer table check constraints ddl = {self.column_constraints_ddl}"
        )
        if self.table_dll or not self.initial_load:
            # can only add constraints to columns if there are any
            # if there is no table_dll an empty table is created and the data schema defines the table
            # on the initial load so this is skipped on the 1st load.
            if self.column_constraints_ddl:
                for cc in self.column_constraints_ddl:
                    self.context.spark.sql(cc)

    def _set_table_properties(self, existing_properties: dict, config: dict):
        _existing_properties = {}
        if existing_properties:
            _existing_properties = existing_properties.get(self.database_table)
            _existing_properties = _existing_properties.get("properties")
        self.tbl_properties_ddl = self._get_table_properties_sql(
            config, _existing_properties
        )
        self.context.log.debug(
            f"Writer table properties ddl = {self.tbl_properties_ddl}"
        )
        if self.tbl_properties_ddl:
            self.context.spark.sql(self.tbl_properties_ddl)

    def create_or_alter_table(self):

        properties = None
        dl.create_database(self.context, self.database)
        table_exists = dl.table_exists(self.context, self.database, self.table)
        if table_exists:
            self.context.log.info(
                f"Table already exists {self.database}.{self.table} at {self.path}"
            )
            self.initial_load = False
            # on non initial loads get the constraints and properties
            # to them to and sync with the declared constraints and properties.
            properties = dl.get_table_properties(
                self.context, self.database, self.table
            )
        else:
            dl.create_table(
                self.context, self.database, self.table, self.path, self.table_dll
            )
            self.initial_load = True

        return properties

    def _get_auto_optimize(self, config: dict):
        return (
            config.get("table")
            and config.get("table").get("properties")
            and config.get("table")
            .get("properties")
            .get("delta.autoOptimize.optimizeWrite")
        )

    def _get_check_constraints_sql(
        self, config: dict, existing_constraints: dict = None
    ):
        table = config.get("table")
        sql_constraints = []
        if table:
            check_constraints = table.get("check_constraints")

        if not check_constraints:
            check_constraints = {}

        # if the existing constraint is not defined in the config constraints
        # and it is different then drop it and recreate
        if existing_constraints:
            for name, existing_constraint in existing_constraints.items():
                defined_constraint = check_constraints.get(name)

                # if the existing constraint is not defined in the config constraints then drop it
                if not defined_constraint:
                    sql_constraints.append(
                        dl.alter_table_drop_constraint(self.database, self.table, name)
                    )
                # if the existing constraint is defined and it is different then drop and add it
                elif (
                    defined_constraint.replace(" ", "").lower()
                    != existing_constraint.replace(" ", "").lower()
                ):
                    sql_constraints.append(
                        dl.alter_table_drop_constraint(self.database, self.table, name)
                    )
                    sql_constraints.append(
                        dl.alter_table_add_constraint(
                            self.database, self.table, name, defined_constraint
                        )
                    )

        # the constraint is defined but doesn't exist on the table yet so
        # add the constraint
        if check_constraints:
            for name, defined_constraint in check_constraints.items():
                existing_constraint = existing_constraints.get(name)
                if not existing_constraint:
                    sql_constraints.append(
                        dl.alter_table_add_constraint(
                            self.database, self.table, name, defined_constraint
                        )
                    )

        return sql_constraints

    def _get_table_properties_sql(self, config: dict, existing_properties: dict = None):

        table = config.get("table")
        if table:
            tbl_properties = table.get("properties")
            if existing_properties:
                tbl_properties = dict(ChainMap(tbl_properties, existing_properties))
            tbl_properties = [f"'{k}' = '{v}'" for k, v in tbl_properties.items()]
            tbl_properties = ", ".join(tbl_properties)
            tbl_properties = dl.alter_table_set_tblproperties(
                self.database, self.table, tbl_properties
            )
            return tbl_properties
        else:
            return None

    def _get_table_sql(self, config: dict):

        table = config.get("table")
        if table:
            ddl: str = table.get("ddl")
            if ddl and os.path.exists(ddl):
                self.schema_repo: ISchemaRepo = (
                    self.context.schema_repo_factory.get_schema_repo_type(
                        self.context, config["deltalake_schema_repo"]
                    )
                )
                ddl = self.schema_repo.load_schema(self.database, self.table)
                ddl = ddl.replace("{{database_name}}", self.database)
                ddl = ddl.replace("{{table_name}}", self.table)
                ddl = ddl.replace("{{path}}", self.path)
        else:
            ddl = None

        return ddl

    def write(self):
        self.context.log.info(f"Writing data to {self.database_table} at {self.path}")
        if self.dataframe:
            (super().write())
            if self.auto_optimize:
                self.context.spark.sql(f"OPTIMIZE `{self.database}`.`{self.table}`")
        else:
            msg = f"Writer dataframe isn't set and cannot be written for {self.database_table} at {self.path}"
            self.context.log.error(msg)
            raise Exception(msg)

    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):

        if not self.options[MERGE_SCHEMA] and value:
            self.options[MERGE_SCHEMA] = value
        self._initial_load = value

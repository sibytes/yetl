from ._destination import Destination
from ..parser._constants import *
from ..schema_repo import ISchemaRepo
import os
from .. import _delta_lake as dl
from pyspark.sql import DataFrame
from typing import ChainMap
from ..parser import parser
from ..save import save_factory, Save


class Writer(Destination):
    def __init__(
        self, context, database: str, table: str, config: dict, io_type: str
    ) -> None:
        super().__init__(context, database, table, config, io_type)

        self.dataframe: DataFrame = None
        # try and load a schema if schema on read
        self.table_ddl: str = self._get_table_sql(config)
        self.zorder_by: list = self._get_table_zorder(config)
        self.context.log.debug(f"Writer table ddl = {self.table_ddl}")

        # get the configured partitions.
        self.partitions: list = self._get_conf_partitions(config, self.table_ddl)
        # get the configured table property for auto optimise.
        self.auto_optimize = self._get_conf_dl_property(
            config, dl.DeltaLakeProperties.OPTIMIZE_WRITE
        )
        # get the configured table property for autocompact - delta.autoOptimize.autoCompact
        self.auto_compact = self._get_conf_dl_property(
            config, dl.DeltaLakeProperties.AUTO_COMPACT
        )

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


        if isinstance(self.mode, dict):
            if "merge" in self.mode.keys():
                mode = self.mode.get("merge")
                self.merge_join = mode.get("join")
                self.merge_update, self.merge_update_match = self._get_merge_match(mode, "update")
                self.merge_insert, self.merge_insert_match = self._get_merge_match(mode, "insert")
                self.merge_delete, self.merge_delete_match = self._get_merge_match(mode, "delete")
                self.mode = "merge"

        self.save:Save = save_factory.get_save_type(self)

        self._initial_load = super().initial_load

        if self.auto_io:
            self.context.log.info(
                f"auto_io = {self.auto_io} automatically creating or altering delta table {self.database}.{self.table}"
            )

            # returns the table properties and partitions if the table already exists.
            current_properties = self.create_or_alter_table()

            # alter, drop or create any constraints defined that are not on the table
            self._set_table_constraints(current_properties, config)
            # alter, drop or create any properties that are not on the table
            self._set_table_properties(current_properties, config)

            # TODO: if the table partitions in the properties form the table is different to the partitions
            # in the config or ddl, yetl.allowRepartitioning = true then repartition the table
            # Is this a feature we want? Not sure.
            self._table_repartition(current_properties, config)

    def _table_repartition(self, table_properties: dict, config: dict):
        pass

    def _get_merge_match(self, mode:dict, crud:str):

        merge = mode.get(crud, False)
        merge_match = None
        if isinstance(merge, dict):
            merge_match = merge.get("match")
            merge = True

        return merge, merge_match

    def _get_conf_partitions(self, config: dict, table_ddl: str):

        # get the partitioned columns from the SQL ddl
        partitions = None
        if table_ddl:
            try:
                partitions = parser.sql_partitioned_by(table_ddl)
                msg = f"Parsed partitioning columns from sql ddl for {self.database}.{self.table} as {partitions}"
                self.context.log.info(msg)
            except Exception as e:
                msg = f"An error has occured parsing sql ddl partitioned clause for {self.database}.{self.table} for the ddl: {table_ddl}"
                self.context.log.error(msg)
                raise Exception(msg) from e

        # if there is no sql ddl then take it from the yaml parameters
        if not partitions:
            table: dict = config[TABLE]
            # otherwise there are no partitioned columns and default to None
            partitions = table.get("partitioned_by", [])
            msg = f"Parsed partitioning columns from dataflow yaml config for {self.database}.{self.table} as {partitions}"

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
        if self.table_ddl or not self.initial_load:
            # can only add constraints to columns if there are any
            # if there is no table_ddl an empty table is created and the data schema defines the table
            # on the initial load so this is skipped on the 1st load.
            if self.column_constraints_ddl:
                for cc in self.column_constraints_ddl:
                    self.context.spark.sql(cc)

    def _set_table_properties(self, existing_properties: dict, config: dict):
        _existing_properties = {}
        if existing_properties:
            _existing_properties = existing_properties.get(self.database_table)
            _existing_properties = _existing_properties.get(PROPERTIES)
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
            # TODO: consolidate details and properties fetch since the properties are in the details. The delta lake api may have some improvements.
            properties = dl.get_table_properties(
                self.context, self.database, self.table
            )
            details = dl.get_table_details(self.context, self.database, self.table)
            # get the partitions from the table details and add them to the properties.
            table_name = f"{self.database}.{self.table}"
            properties[table_name][PARTITIONS] = details[table_name][PARTITIONS]

        else:
            dl.create_table(
                self.context, self.database, self.table, self.path, self.table_ddl
            )
            self.initial_load = True

        return properties

    def _get_conf_dl_property(self, config: dict, property: dl.DeltaLakeProperties):
        return (
            config.get(TABLE)
            and config.get(TABLE).get(PROPERTIES)
            and config.get(TABLE).get(PROPERTIES).get(property.value)
        )

    def _get_check_constraints_sql(
        self, config: dict, existing_constraints: dict = None
    ):
        table = config.get(TABLE)
        sql_constraints = []
        if table:
            check_constraints = table.get(CHECK_CONSTRAINTS)

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
            tbl_properties = table.get(PROPERTIES)
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

    def _get_table_zorder(self, config: dict):

        table = config.get(TABLE)
        zorder_by: list = []
        if table:
            zorder_by = table.get("zorder_by", [])

        return zorder_by

    def _get_table_sql(self, config: dict):

        table = config.get(TABLE)
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

            # don't think this is needed because from the docs
            # "Repartition output data before write: For partitioned tables, merge can produce a much larger number 
            # of small files than the number of shuffle partitions. This is because every shuffle task can write 
            # multiple files in multiple partitions, and can become a performance bottleneck. In many cases, it 
            # helps to repartition the output data by the table’s partition columns before writing it. You enable 
            # this by setting the Spark session configuration spark.databricks.delta.merge.repartitionBeforeWrite.enabled to true."

            # auto_compact = all(
            #     [self.auto_compact, self.partitions, not self.context.is_databricks]
            # )
            # if auto_compact:
            #     self.context.log.info(
            #         f"Auto compacting in memory partitions for {self.database_table} on partitions {self.partitions}"
            #     )
            #     self.dataframe = self.dataframe.repartition(
            #         *self.partitions
            #     )

            super().write()

            auto_optimize = all([self.auto_optimize, not self.context.is_databricks])
            if auto_optimize:
                self.context.log.info(
                    f"Auto optimizing {self.database_table} where {self.partition_values} zorder by {self.zorder_by}"
                )
                dl.optimize(
                    self.context,
                    self.database,
                    self.table,
                    self.partition_values,
                    self.zorder_by,
                )

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

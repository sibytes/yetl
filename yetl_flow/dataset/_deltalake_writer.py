from ..parser._constants import *
from ..schema_repo import ISchemaRepo
import os
from .. import _delta_lake as dl
from pyspark.sql import DataFrame
from typing import ChainMap
from ..parser import parser
from ..save import save_factory, Save
from ..audit import Audit, AuditTask
from datetime import datetime
from pyspark.sql import functions as fn
import json
from ._dataset import Dataset
from ._base import Destination


class DeltaWriter(Dataset, Destination):
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

        self._replacements = {
            parser.JinjaVariables.DATABASE_NAME:self.database, 
            parser.JinjaVariables.TABLE_NAME:self.table, 
            parser.JinjaVariables.PATH:self.path 
        }

        self.dataframe: DataFrame = None
        # try and load a schema if schema on read
        self.table_ddl: str = self._get_table_sql(config)
        self.zorder_by: list = self._get_table_zorder(config)
        self.context.log.debug(f"DeltaWriter table ddl = {self.table_ddl}")

        # get the configured partitions.
        self.partitions: list = self._get_conf_partitions(config, self.table_ddl)
        # get the configured table property for auto optimise.
        self.auto_optimize = self._get_conf_dl_property(
            config, dl.DeltaLakeProperties.OPTIMIZE_WRITE
        )

        # gets the read, write, etc options based on the type
        io_properties = config.get("write")
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
                self.merge_update, self.merge_update_match = self._get_merge_match(
                    mode, "update"
                )
                self.merge_insert, self.merge_insert_match = self._get_merge_match(
                    mode, "insert"
                )
                self.merge_delete, self.merge_delete_match = self._get_merge_match(
                    mode, "delete"
                )
                self.mode = "merge"

        self.save: Save = save_factory.get_save_type(self)

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

    def _get_merge_match(self, mode: dict, crud: str):

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
                start_datetime = datetime.now()
                for cc in self.column_constraints_ddl:
                    self.context.spark.sql(cc)
                self.auditor.dataset_task(
                    self.id,
                    AuditTask.SET_TABLE_PROPERTIES,
                    self.column_constraints_ddl,
                    start_datetime,
                )

    def _set_table_properties(self, existing_properties: dict, config: dict):
        _existing_properties = {}
        if existing_properties:
            _existing_properties = existing_properties.get(self.database_table)
            _existing_properties = _existing_properties.get(PROPERTIES)
        self.tbl_properties_ddl = self._get_table_properties_sql(
            config, _existing_properties
        )
        self.context.log.debug(
            f"DeltaWriter table properties ddl = {self.tbl_properties_ddl}"
        )
        if self.tbl_properties_ddl:
            start_datetime = datetime.now()
            self.context.spark.sql(self.tbl_properties_ddl)
            self.auditor.dataset_task(
                self.id,
                AuditTask.SET_TABLE_PROPERTIES,
                self.tbl_properties_ddl,
                start_datetime,
            )

    def create_or_alter_table(self):

        properties = None
        start_datetime = datetime.now()
        detail = dl.create_database(self.context, self.database)
        self.auditor.dataset_task(self.id, AuditTask.SQL, detail, start_datetime)

        table_exists = dl.table_exists(self.context, self.database, self.table)
        if table_exists:
            self.context.log.info(
                f"Table already exists {self.database}.{self.table} at {self.path}"
            )
            self.initial_load = False

            start_datetime = datetime.now()
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
            self.auditor.dataset_task(
                self.id, AuditTask.GET_TABLE_PROPERTIES, properties, start_datetime
            )

        else:
            start_datetime = datetime.now()
            detail = dl.create_table(
                self.context, self.database, self.table, self.path, self.table_ddl
            )
            self.auditor.dataset_task(self.id, AuditTask.SQL, detail, start_datetime)
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
            if ddl and not "\n" in ddl:
                self.schema_repo: ISchemaRepo = (
                    self.context.schema_repo_factory.get_schema_repo_type(
                        self.context, config["deltalake_schema_repo"]
                    )
                )
                ddl = self.schema_repo.load_schema(self.database, self.table, ddl)
                ddl = parser.render_jinja(ddl, self._replacements)
        else:
            ddl = None

        return ddl

    def _get_partitions_values(self):

        partition_values = {}
        if self.partitions:
            partition_values_df = self.dataframe.select(*self.partitions).distinct()

            for p in self.partitions:
                group_by: list = list(self.partitions)
                group_by.remove(p)
                if group_by:
                    partition_values_df = partition_values_df.groupBy(*group_by).agg(
                        fn.collect_set(p).alias(p)
                    )
                else:
                    partition_values_df = partition_values_df.withColumn(
                        p, fn.collect_set(p)
                    )

            partition_values_df = partition_values_df.collect()
            partition_values = partition_values_df[0].asDict()

        return partition_values

    def _prepare_write(self):

        self.context.log.debug(
            f"Reordering sys_columns to end for {self.database_table} from {self.path} {CONTEXT_ID}={str(self.context_id)}"
        )
        # remove a re-add the _context_id since there will be dupplicate columns
        # when dataframe is built from multiple sources.
        self.dataframe = self.dataframe.drop(CONTEXT_ID).withColumn(
            CONTEXT_ID, fn.lit(str(self.context_id))
        )

        # clean up the column ordering
        sys_columns = [c for c in self.dataframe.columns if c.startswith("_")]
        data_columns = [c for c in self.dataframe.columns if not c.startswith("_")]
        data_columns = data_columns + sys_columns
        self.dataframe = self.dataframe.select(*data_columns)

        # get the partitions values for efficient IO patterns
        self.partition_values = self._get_partitions_values()

        if self.partition_values:
            msg_partition_values = json.dumps(self.partition_values, indent=4)
            self.context.log.info(
                f"""IO operations for {self.database}.{self.table} will be paritioned by: \n{msg_partition_values}"""
            )

    def write(self):
        self.context.log.info(f"Writing data to {self.database_table} at {self.path}")
        if self.dataframe:

            # don't think this is needed because from the docs
            # "Repartition output data before write: For partitioned tables, merge can produce a much larger number
            # of small files than the number of shuffle partitions. This is because every shuffle task can write
            # multiple files in multiple partitions, and can become a performance bottleneck. In many cases, it
            # helps to repartition the output data by the table’s partition columns before writing it. You enable
            # this by setting the Spark session configuration spark.databricks.delta.merge.repartitionBeforeWrite.enabled to true."
            start_datetime = datetime.now()
            self._prepare_write()
            self.save.write()
            write_audit = dl.get_audit(self.context, f"{self.database}.{self.table}")
            self.auditor.dataset_task(
                self.id, AuditTask.DELTA_TABLE_WRITE, write_audit, start_datetime
            )

            auto_optimize = all([self.auto_optimize, not self.context.is_databricks])
            if auto_optimize:
                self.context.log.info(
                    f"Auto optimizing {self.database_table} where {self.partition_values} zorder by {self.zorder_by}"
                )
                start_datetime = datetime.now()
                dl.optimize(
                    self.context,
                    self.database,
                    self.table,
                    self.partition_values,
                    self.zorder_by,
                )
                write_audit = dl.get_audit(
                    self.context, f"{self.database}.{self.table}"
                )
                self.auditor.dataset_task(
                    self.id, AuditTask.DELTA_TABLE_OPTIMIZE, write_audit, start_datetime
                )

        else:
            msg = f"DeltaWriter dataframe isn't set and cannot be written for {self.database_table} at {self.path}"
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

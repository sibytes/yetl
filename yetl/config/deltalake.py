import json
from pyspark.sql import DataFrame
import logging
from pyspark.sql.types import StructType, StructField
import jinja2
from typing import List, Union, Dict
from ._spark_context import get_spark_context
from pydantic import BaseModel, Field
from typing import Any
from ._project import Project
from pyspark.sql import SparkSession


_logger = logging.getLogger(__name__)


class DeltaLakeFn(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.spark = get_spark_context(self.project.name, self.project.spark.config)

    project: Project = Field(...)
    spark: SparkSession = Field(default=None)

    def get_partition_predicate(self, partition_values: dict):
        predicates = []
        for k, v in partition_values.items():
            v = [str(val) for val in v]
            p = f"`{k}` in ({','.join(v)})"
            predicates.append(p)

        predicate = " and ".join(predicates)

        return predicate

    def table_exists(self, database: str, table: str):
        table_exists = (
            self.spark.sql(f"SHOW TABLES in {database};")
            .where(f"tableName='{table}' AND !isTemporary")
            .count()
            == 1
        )
        return table_exists

    def get_delta_properties_sql(self, delta_properties: Dict[str, Union[str, bool]]):
        sql_properties = [
            f"{k.lower()} = {v.lower()}" for k, v in delta_properties.items()
        ]
        sql_properties = ", ".join(sql_properties)
        return sql_properties

    def create_table(
        self,
        database: str,
        table: str,
        path: str = None,
        delta_properties: List[str] = None,
        sql: str = None,
    ):
        _logger.debug(f"Creating table if not exists {database}.{table} at {path}")

        if not sql:
            sql = f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}`"

            # add in the delta properties if there are any
            sql_properties = ""
            if delta_properties:
                sql_properties = self.get_delta_properties_sql(delta_properties)
                sql_properties = f"TBLPROPERTIES({sql_properties})"

            sql_path = ""
            if path:
                sql_path = f"USING DELTA LOCATION '{path}'"

                sql = f"{sql}\n{sql_path}\n{sql_properties};"

        _logger.debug(f"{sql}")
        print(sql)
        self.spark.sql(sql)

        return sql

    def create_database(self, database: str):
        _logger.debug(f"Creating database if not exists `{database}`")
        sql = f"CREATE DATABASE IF NOT EXISTS {database}"
        _logger.debug(sql)
        self.spark.sql(sql)
        return sql

    def alter_table_drop_constraint(self, database: str, table: str, name: str):
        return f"ALTER TABLE `{database}`.`{table}` DROP CONSTRAINT {name};"

    def alter_table_add_constraint(
        self, database: str, table: str, name: str, constraint: str
    ):
        return f"ALTER TABLE `{database}`.`{table}` ADD CONSTRAINT {name} CHECK ({constraint});"

    def alter_table_set_tblproperties(self, database: str, table: str, properties: str):
        return f"ALTER TABLE `{database}`.`{table}` SET TBLPROPERTIES ({properties});"

    def get_table_properties(self, database: str, table: str):
        _logger.debug(f"getting existing table properties for table {database}.{table}")

        df: DataFrame = self.spark.sql(
            f"SHOW TBLPROPERTIES `{database}`.`{table}`"
        ).collect()
        tbl_properties = {
            c.asDict()["key"]: c.asDict()["value"]
            for c in df
            if not c["key"].startswith("delta.constraints")
        }
        tbl_constraints = {
            c.asDict()["key"].split(".")[-1]: c.asDict()["value"]
            for c in df
            if c["key"].startswith("delta.constraints")
        }

        properties = {
            f"{database}.{table}": {
                "constraints": tbl_constraints,
                "properties": tbl_properties,
            }
        }
        msg = json.dumps(properties, indent=4, default=str)
        _logger.debug(msg)
        return properties

    def optimize(
        self, database: str, table: str, partition_values: dict, zorder_by: list = []
    ):
        sql = f"OPTIMIZE `{database}`.`{table}`"

        if partition_values:
            predicate = self.get_partition_predicate(partition_values)
            predicate = f" WHERE {predicate}"
            sql = f"{sql}{predicate}"

        if zorder_by:
            sql_zorderby = ",".join([f"`{z}`" for z in zorder_by])
            sql = f"{sql} ZORDER BY ({sql_zorderby})"

        _logger.debug(f"optimizing table {database}.{table}\n{sql}")
        self.spark.sql(sql)

    def get_table_details(self, database: str, table: str):
        _logger.debug(
            f"getting existing table details and partitions for table {database}.{table}"
        )

        df: DataFrame = self.spark.sql(
            f"DESCRIBE TABLE EXTENDED `{database}`.`{table}`"
        ).collect()

        # get the details into a dictionary
        details = {c.asDict()["col_name"]: c.asDict()["data_type"] for c in df}

        # pull out the columns
        columns = {}
        ordinal = 0
        for k, v in details.items():
            if k and v:
                columns[k] = {}
                columns[k]["ordinal"] = ordinal
                columns[k]["type"] = v
                ordinal = +1
            else:
                break

        # pull out the columns
        partitions = [v for k, v in details.items() if k.startswith("Part ")]

        details = {
            f"{database}.{table}": {
                "columns": columns,
                "partitions": partitions,
                "name": details.get("Name"),
                "location": details.get("Location"),
                "provider": details.get("Provider"),
                "owner": details.get("Owner"),
            }
        }

        msg = json.dumps(details, indent=4, default=str)
        _logger.debug(msg)
        return details

    def create_column_ddl(self, field: StructField):
        nullable = "" if field.nullable else "NOT NULL"
        comment = f"COMMENT {field.metadata}" if field.metadata else ""
        field_type = field.dataType.typeName()
        field_name = f"`{field.name}`"

        return f"\t{field_name} {field_type} {nullable} {comment}"

    def create_table_dll(
        self,
        schema: StructType,
        partition_fields: list = [],
        format: str = "DELTA",
        always_identity_column: str = None,
    ):
        field_ddl = [self.create_column_ddl(f) for f in schema.fields]
        if always_identity_column:
            always_identity_column = (
                f"\t`{always_identity_column}` GENERATED ALWAYS AS IDENTITY"
            )
            field_ddl = [always_identity_column] + field_ddl

        field_ddl = ",\n".join(field_ddl)

        template_partition = jinja2.Template("PARTITIONED BY ({{partition_fields}})")
        template_ddl = jinja2.Template(
            """CREATE TABLE {{database_name}}.{{table_name}}
    (
    {{field_ddl}}
    )
    USING {{format}} LOCATION '{{path}}'
    {{partition_ddl}}""",
            undefined=jinja2.DebugUndefined,
        )

        if partition_fields:
            partition_fields = [f"`{p}`" for p in partition_fields]
            partition_fields = ",".join(partition_fields)
            partition_ddl: str = template_partition.render(
                partition_fields=partition_fields
            )
        else:
            partition_ddl = ""

        replace = {
            "field_ddl": field_ddl,
            "partition_ddl": partition_ddl,
            "format": format,
        }

        table_ddl = template_ddl.render(replace)
        table_ddl = f"{table_ddl};"

        return table_ddl

    class Config:
        arbitrary_types_allowed = True

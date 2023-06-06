import json
from pyspark.sql import DataFrame
import logging
from pyspark.sql.types import StructType, StructField
import jinja2
from typing import List, Union, Dict
from ._spark_context import get_spark_context
from pydantic import BaseModel, Field, PrivateAttr
from typing import Any
from ._project import Project
from pyspark.sql import SparkSession
import re


class DeltaLakeFn(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        self.spark = get_spark_context(self.project.name, self.project.spark.config)

    _logger: Any = PrivateAttr(default=None)
    project: Project = Field(...)
    spark: SparkSession = Field(default=None)

    @classmethod
    def to_regex_search_pattern(cls, py_format: str):
        """Convert python format codes to regex search pattern
           these can be used to intelligently strip timeslice from file or name
           see here for an exampl
           https://github.com/sibytes/yetl_archive/blob/main/yetl/flow/dataset/_reader.py#L387

            (\d{1,4}([.\-/])\d{1,2}([.\-/])\d{1,4})  # noeq W605
        https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

        %d -> \d{2}       - Day of the month as a zero-padded decimal number.
        %m -> [0-1][1-9]  - Month as a zero-padded decimal number.
        %y -> \d{2}       - Year without century as a zero-padded decimal number.
        %Y -> \d{4}       - Year with century as a decimal number.
        %H -> [0-2][1-9]  - Hour (24-hour clock) as a zero-padded decimal number.
        %M -> \d{2}       - Minute as a zero-padded decimal number.
        %S -> \d{2}       - Second as a zero-padded decimal number.
        %f -> [0]\.?\d{6} - Microsecond as a decimal number, zero-padded to 6 digits.
        %% -> ([%])       - A literal '%' character.
        %j -> \d{2}       - Day of the year as a zero-padded decimal number.

        NOT SUPPORTED - %U - Week number of the year (Sunday as the first day of the week) as a zero-padded decimal number. All days in a new year preceding the first Sunday are considered to be in week 0.
        NOT SUPPORTED - %W - Week number of the year (Monday as the first day of the week) as a zero-padded decimal number. All days in a new year preceding the first Monday are considered to be in week 0.

        NOT SUPPORTED - %c - Locale’s appropriate date and time representation.
        NOT SUPPORTED - %x - Locale’s appropriate date representation.
        NOT SUPPORTED - %X - Locale’s appropriate time representation.
        NOT SUPPORTED - %G - ISO 8601 year with century representing the year that contains the greater part of the ISO week (%V).
        NOT SUPPORTED - %u - ISO 8601 weekday as a decimal number where 1 is Monday.
        NOT SUPPORTED - %V - ISO 8601 week as a decimal number with Monday as the first day of the week. Week 01 is the week containing Jan 4.
        NOT SUPPORTED - %z - UTC offset in the form ±HHMM[SS[.ffffff]] (empty string if the object is naive).
        NOT SUPPORTED - %Z - Time zone name (empty string if the object is naive).
        NOT SUPPORTED - %I - Hour (12-hour clock) as a zero-padded decimal number.
        NOT SUPPORTED - %p - Locale’s equivalent of either AM or PM.
        NOT SUPPORTED - %b - Month as locale’s abbreviated name.
        NOT SUPPORTED - %B - Month as locale’s full name.
        NOT SUPPORTED - %a - Weekday as locale’s abbreviated name.
        NOT SUPPORTED - %A - Weekday as locale’s full name.
        NOT SUPPORTED - %w - Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.
        """  # noqa W605

        UNSUPPORTED_FORMAT_CODES = [
            "%U",
            "%W",
            "%c",
            "%x",
            "%X",
            "%G",
            "%u",
            "%V",
            "%z",
            "%Z",
            "%I",
            "%p",
            "%b",
            "%B",
            "%a",
            "%A",
            "%w",
        ]
        unsupported_codes = []
        for c in UNSUPPORTED_FORMAT_CODES:
            if c in py_format:
                unsupported_codes.append(c)

        if unsupported_codes:
            unsupported_codes = ",".join(unsupported_codes)
            raise Exception(
                f"The format contains the following unsupported format codes: {unsupported_codes}"
            )

        pattern = py_format.replace("%d", "\d{2}")  # noqa W605
        pattern = pattern.replace("%m", "[0-1][1-9]")  # noqa W605
        pattern = pattern.replace("%y", "\d{2}")  # noqa W605
        pattern = pattern.replace("%Y", "\d{4}")  # noqa W605
        pattern = pattern.replace("%H", "[0-2][1-9]")  # noqa W605
        pattern = pattern.replace("%M", "\d{2}")  # noqa W605
        pattern = pattern.replace("%S", "\d{2}")  # noqa W605
        pattern = pattern.replace("%f", "[0]\.?\d{6}")  # noqa W605

        seps = [f"%{s[0]}" for s in pattern.split("%")][1:]
        replace_seps = [s[0] for s in pattern.split("%")][1:]
        replace_seps = [f"[{s}]" for s in replace_seps]

        for s, r in zip(seps, replace_seps):
            pattern = pattern.replace(s, r)

        pattern = f"({pattern})"

        return pattern

    @classmethod
    def to_spark_format_code(cls, py_format: str):
        """Convert python format codes to spark format codes

        https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

        %d -> dd    - Day of the month as a zero-padded decimal number.
        %m -> MM    - Month as a zero-padded decimal number.
        %y -> yy    - Year without century as a zero-padded decimal number.
        %Y -> yyyy  - Year with century as a decimal number.
        %H -> HH    - Hour (24-hour clock) as a zero-padded decimal number.
        %M -> mm    - Minute as a zero-padded decimal number.
        %S -> s     - Second as a zero-padded decimal number.
        %f -> S     - Microsecond as a decimal number, zero-padded to 6 digits.
        %% -> %     - A literal '%' character.
        %j -> DD    - Day of the year as a zero-padded decimal number.

        NOT SUPPORTED - %U - Week number of the year (Sunday as the first day of the week) as a zero-padded decimal number. All days in a new year preceding the first Sunday are considered to be in week 0.
        NOT SUPPORTED - %W - Week number of the year (Monday as the first day of the week) as a zero-padded decimal number. All days in a new year preceding the first Monday are considered to be in week 0.

        NOT SUPPORTED - %c - Locale’s appropriate date and time representation.
        NOT SUPPORTED - %x - Locale’s appropriate date representation.
        NOT SUPPORTED - %X - Locale’s appropriate time representation.
        NOT SUPPORTED - %G - ISO 8601 year with century representing the year that contains the greater part of the ISO week (%V).
        NOT SUPPORTED - %u - ISO 8601 weekday as a decimal number where 1 is Monday.
        NOT SUPPORTED - %V - ISO 8601 week as a decimal number with Monday as the first day of the week. Week 01 is the week containing Jan 4.
        NOT SUPPORTED - %z - UTC offset in the form ±HHMM[SS[.ffffff]] (empty string if the object is naive).
        NOT SUPPORTED - %Z - Time zone name (empty string if the object is naive).
        NOT SUPPORTED - %I - Hour (12-hour clock) as a zero-padded decimal number.
        NOT SUPPORTED - %p - Locale’s equivalent of either AM or PM.
        NOT SUPPORTED - %b - Month as locale’s abbreviated name.
        NOT SUPPORTED - %B - Month as locale’s full name.
        NOT SUPPORTED - %a - Weekday as locale’s abbreviated name.
        NOT SUPPORTED - %A - Weekday as locale’s full name.
        NOT SUPPORTED - %w - Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.
        """

        UNSUPPORTED_FORMAT_CODES = [
            "%U",
            "%W",
            "%c",
            "%x",
            "%X",
            "%G",
            "%u",
            "%V",
            "%z",
            "%Z",
            "%I",
            "%p",
            "%b",
            "%B",
            "%a",
            "%A",
            "%w",
        ]
        unsupported_codes = []
        for c in UNSUPPORTED_FORMAT_CODES:
            if c in py_format:
                unsupported_codes.append(c)

        if unsupported_codes:
            unsupported_codes = ",".join(unsupported_codes)
            raise Exception(
                f"The format contains the following unsupported format codes: {unsupported_codes}"
            )

        py_format = py_format.replace("%d", "dd")
        py_format = py_format.replace("%m", "MM")
        py_format = py_format.replace("%y", "yy")
        py_format = py_format.replace("%Y", "yyyy")
        py_format = py_format.replace("%H", "HH")
        py_format = py_format.replace("%M", "mm")
        py_format = py_format.replace("%S", "s")
        py_format = py_format.replace("%f", "S")
        py_format = py_format.replace("%", "")

        return py_format

    @classmethod
    def reduce_whitespace(cls, sentence: str):
        pattern = re.compile(r"\s+")

        return pattern.sub(" ", sentence).strip()

    def get_partition_predicate(self, partition_values: dict):
        predicates = []
        for k, v in partition_values.items():
            v = [str(val) for val in v]
            p = f"`{k}` in ({','.join(v)})"
            predicates.append(p)

        predicate = " and ".join(predicates)

        return predicate

    def table_exists(self, database: str, table: str, catalog: str = None):
        database = f"`{catalog}`.`{database}`" if catalog else f"`{database}`"
        _table_exists = (
            self.spark.sql(f"SHOW TABLES in {database};")
            .where(f"tableName='{table}' AND !isTemporary")
            .count()
            == 1
        )
        # Not whitelist on databricks.
        # table_exists = self.spark.catalog.tableExists(f"`{database}`.`{table}`")
        return _table_exists

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
        catalog: str = None,
    ):
        database = f"`{catalog}`.`{database}`" if catalog else f"`{database}`"
        table = f"`{table}`"
        self._logger.info(f"Creating table if not exists {database}.{table} at {path}")
        if not sql:
            sql = f"CREATE TABLE IF NOT EXISTS {database}.{table}"

            # add in the delta properties if there are any
            sql_properties = ""
            if delta_properties:
                sql_properties = self.get_delta_properties_sql(delta_properties)
                sql_properties = f"TBLPROPERTIES({sql_properties})"

            sql_path = ""
            if path:
                sql_path = f"USING DELTA LOCATION '{path}'"

                sql = f"{sql}\n{sql_path}\n{sql_properties};"

        self._logger.info(f"{sql}")
        self.spark.sql(sql)

        return sql

    def create_database(self, database: str, catalog: str = None):
        database = f"`{catalog}`.`{database}`" if catalog else f"`{database}`"
        self._logger.debug(f"Creating database if not exists {database}")
        sql = f"CREATE DATABASE IF NOT EXISTS {database}"
        self._logger.debug(sql)
        self.spark.sql(sql)
        return sql

    def alter_table_drop_constraint(
        self, database: str, table: str, name: str, catalog: str = None
    ):
        database = f"`{catalog}`.`{database}`" if catalog else f"`{database}`"
        return f"ALTER TABLE {database}.`{table}` DROP CONSTRAINT {name};"

    def alter_table_add_constraint(
        self, database: str, table: str, name: str, constraint: str, catalog: str = None
    ):
        database = f"`{catalog}`.`{database}`" if catalog else f"`{database}`"
        return f"ALTER TABLE {database}.`{table}` ADD CONSTRAINT {name} CHECK ({constraint});"

    def alter_table_set_tblproperties(
        self, database: str, table: str, properties: str, catalog: str = None
    ):
        database = f"`{catalog}`.`{database}`" if catalog else f"`{database}`"
        return f"ALTER TABLE {database}.`{table}` SET TBLPROPERTIES ({properties});"

    def get_table_properties(self, database: str, table: str, catalog: str = None):
        database = f"`{catalog}`.`{database}`" if catalog else f"`{database}`"
        self._logger.debug(
            f"getting existing table properties for table {database}.`{table}`"
        )

        df: DataFrame = self.spark.sql(
            f"SHOW TBLPROPERTIES {database}.`{table}`"
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
        self._logger.debug(msg)
        return properties

    def optimize(
        self,
        database: str,
        table: str,
        partition_values: dict,
        zorder_by: list = [],
        catalog: str = None,
    ):
        database = f"`{catalog}`.`{database}`" if catalog else f"`{database}`"
        sql = f"OPTIMIZE {database}.`{table}`"

        if partition_values:
            predicate = self.get_partition_predicate(partition_values)
            predicate = f" WHERE {predicate}"
            sql = f"{sql}{predicate}"

        if zorder_by:
            sql_zorderby = ",".join([f"`{z}`" for z in zorder_by])
            sql = f"{sql} ZORDER BY ({sql_zorderby})"

        self._logger.info(f"optimizing table {database}.{table}\n{sql}")
        self.spark.sql(sql)

    def get_table_details(self, database: str, table: str, catalog: str = None):
        database = f"`{catalog}`.`{database}`" if catalog else f"`{database}`"
        self._logger.debug(
            f"getting existing table details and partitions for table {database}.{table}"
        )

        df: DataFrame = self.spark.sql(
            f"DESCRIBE TABLE EXTENDED {database}.`{table}`"
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
        self._logger.debug(msg)
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

from dataclasses import replace
from enum import Enum
from pipes import Template
import re
from datetime import datetime
import regex
from ._constants import NAME, REPLACE, ARGS
import jinja2
from pyspark.sql.types import StructType, StructField


class JinjaVariables(Enum):
    DATABASE_NAME = "database_name"
    TABLE_NAME = "table_name"
    PATH = "path"
    ROOT = "root"
    TIMESLICE_FILE_DATE_FORMAT = "timeslice_file_date_format"
    TIMESLICE_PATH_DATE_FORMAT = "timeslice_path_date_format"


class TimeslicePosition:
    def __init__(
        self, start_pos: int, end_pos: int, length: int, format_code: str
    ) -> None:
        self.start_pos = start_pos
        self.end_pos = end_pos
        self.length = length
        self.format_code = format_code


def sql_partitioned_by(sql: str):

    white_space = re.compile(r"\s+")
    sql = re.sub(white_space, "", sql).lower()

    partitioned_by = r"partitionedby\((.*)\)"
    partitions = re.search(partitioned_by, sql)
    if partitions:
        partitions_clause = partitions.group()
        partitions_clause = partitions_clause.replace("(", "")
        partitions_clause = partitions_clause.replace(")", "")
        partitions_clause = partitions_clause.replace("partitionedby", "")
        partitions_lst = partitions_clause.split(",")
        partitions_lst = [p.replace("`", "") for p in partitions_lst]
        return partitions_lst
    else:
        return None


def to_regex_search_pattern(py_format: str):
    """Convert python format codes to spark format codes
        (\d{1,4}([.\-/])\d{1,2}([.\-/])\d{1,4})
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
    NOT SUPPORTED - %w - Weekday as a decimal number, where 0 is Sunday and 6 is Saturday."""

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

    pattern = py_format.replace("%d", "\d{2}")
    pattern = pattern.replace("%m", "[0-1][1-9]")
    pattern = pattern.replace("%y", "\d{2}")
    pattern = pattern.replace("%Y", "\d{4}")
    pattern = pattern.replace("%H", "[0-2][1-9]")
    pattern = pattern.replace("%M", "\d{2}")
    pattern = pattern.replace("%S", "\d{2}")
    pattern = pattern.replace("%f", "[0]\.?\d{6}")

    seps = [f"%{s[0]}" for s in pattern.split("%")][1:]
    replace_seps = [s[0] for s in pattern.split("%")][1:]
    replace_seps = [f"[{s}]" for s in replace_seps]

    for s, r in zip(seps, replace_seps):
        pattern = pattern.replace(s, r)

    pattern = f"({pattern})"

    return pattern


def to_spark_format_code(py_format: str):
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
    NOT SUPPORTED - %w - Weekday as a decimal number, where 0 is Sunday and 6 is Saturday."""

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


def render_jinja(data: str, replacements: dict[JinjaVariables, str]):

    if data and isinstance(data, str):
        replace = {k.value: v for (k, v) in replacements.items()}
        template: jinja2.Template = jinja2.Template(data)
        data = template.render(replace)

    return data


def reduce_whitespace(sentence: str):

    pattern = re.compile(r"\s+")

    return pattern.sub(" ", sentence).strip()


def create_column_ddl(field: StructField):
    nullable = "" if field.nullable else "NOT NULL"
    comment = f"COMMENT {field.metadata}" if field.metadata else ""
    field_type = field.dataType.typeName()
    field_name = f"`{field.name}`"

    return f"\t{field_name} {field_type} {nullable} {comment}"


def create_table_dll(
    schema: StructType,
    partition_fields: list = [],
    format: str = "DELTA",
    always_identity_column: str = None,
):

    field_ddl = [create_column_ddl(f) for f in schema.fields]
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

    replace = {"field_ddl": field_ddl, "partition_ddl": partition_ddl, "format": format}

    table_ddl = template_ddl.render(replace)
    table_ddl = f"{table_ddl};"

    return table_ddl

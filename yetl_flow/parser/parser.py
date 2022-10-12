from enum import Enum
from pipes import Template
import re
from datetime import datetime
import regex
from ._constants import NAME, REPLACE, ARGS
import jinja2


class JinjaVariables(Enum):
    DATABASE_NAME = "database_name"
    TABLE_NAME = "table_name"
    PATH = "path"
    ROOT = "root"

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
        return partitions_lst
    else:
        return None


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


def render_jinja(data:str, replacements:dict[JinjaVariables, str]):

    if data and isinstance(data, str):
        replace = {k.value:v for (k, v) in replacements.items()}
        template:jinja2.Template = jinja2.Template(data)
        data = template.render(replace)
    
    return data


def parse_functions(sentence: str):

    rgx_function = regex.compile(r"(?si)(?|{0}(.*?){1}|{1}(.*?){0})".format("{{", "}}"))
    functions = rgx_function.findall(sentence)
    funcs = []
    for f in functions:
        for fp in f:
            func: str = fp
            args = func[func.find("(") + 1 : func.find(")")]
            args = args.split(",")
            args = [a.strip() for a in args]
            function_name = func[: func.find("(")].strip()
            function = {NAME: function_name, REPLACE: func, ARGS: args}
            funcs.append(function)

    return funcs


def get_slice_position(path: str, dataset):

    parsed = parse_functions(path)
    start_pos = path.find("{")
    end_pos = -1
    timeslice_pos = None

    if start_pos > 0:
        dte_format_attribute = parsed[0]["args"][0]

        if hasattr(dataset, dte_format_attribute):
            format_str = getattr(dataset, dte_format_attribute)

            l = len(datetime.now().strftime(format_str))

            end_pos = start_pos + l
            format_str = to_spark_format_code(format_str)

            timeslice_pos = TimeslicePosition(start_pos + 3, end_pos + 3, l, format_str)

    return timeslice_pos


def reduce_whitespace(sentence: str):

    pattern = re.compile(r"\s+")

    return pattern.sub(" ", sentence).strip()

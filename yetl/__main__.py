import typer
from .cli import _init
from enum import Enum
from typing_extensions import Annotated
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from .config._spark_context import get_spark_context
app = typer.Typer()


class ImportFormat(str, Enum):
    excel = "excel"


class ColumnNames(str, Enum):
    stage = "stage"
    table_type = "table_type"
    database = "database"
    table = "table"
    sql = "sql"
    ids = "ids"
    depends_on = "depends_on"
    deltalake = "deltalake"
    identity = "identity"
    partition_by = "partition_by"
    delta_constraints = "delta_constraints"
    z_order_by = "z_order_by"
    delta_properties = "delta_properties"
    error_thresholds = "error_thresholds"
    warning_thresholds = "warning_thresholds"
    invalid_ratio = "invalid_ratio"
    invalid_rows = "invalid_rows"
    max_rows = "max_rows"
    mins_rows = "mins_rows"
    custom_properties = "custom_properties"


SCHEMA = {
    ColumnNames.stage: str,
    ColumnNames.table_type: str,
    ColumnNames.database: str,
    ColumnNames.table: str,
    ColumnNames.sql: str,
    ColumnNames.ids: str,
    ColumnNames.depends_on: str,
    f"{ColumnNames.deltalake}.{ColumnNames.delta_properties}": str,
    f"{ColumnNames.deltalake}.{ColumnNames.identity}": str,
    f"{ColumnNames.deltalake}.{ColumnNames.partition_by}": str,
    f"{ColumnNames.deltalake}.{ColumnNames.delta_constraints}": str,
    f"{ColumnNames.deltalake}.{ColumnNames.z_order_by}": str,
    f"{ColumnNames.warning_thresholds}.{ColumnNames.invalid_ratio}": np.float64,
    f"{ColumnNames.warning_thresholds}.{ColumnNames.invalid_rows}": np.float64,
    f"{ColumnNames.warning_thresholds}.{ColumnNames.max_rows}": np.float64,
    f"{ColumnNames.warning_thresholds}.{ColumnNames.mins_rows}": np.float64,
    f"{ColumnNames.error_thresholds}.{ColumnNames.invalid_ratio}": np.float64,
    f"{ColumnNames.error_thresholds}.{ColumnNames.invalid_rows}": np.float64,
    f"{ColumnNames.error_thresholds}.{ColumnNames.max_rows}": np.float64,
    f"{ColumnNames.error_thresholds}.{ColumnNames.mins_rows}": np.float64,
    # "custom_properties.process_group": np."float64",
    # "custom_properties.rentention_days": np."float64",
    # "custom_properties.vaccum": np."float64"
}


def validate_schema(df: pd.DataFrame):
    schema_exceptions = []
    file_schema = {}
    for column in df:
        if df[column].name[0] != "custom_properties":
            if df[column].dtype.type == np.object_:
                df[column] = df[column].astype(pd.StringDtype())
            name = [n for n in df[column].name if not n.startswith("Unnamed:")]
            name = ".".join(name)
            file_schema[name] = df[column].dtype.type

    for name, data_type in SCHEMA.items():
        if name not in file_schema:
            schema_exceptions.append(
                f"invalid schema column name {name} with type {data_type}"
            )

        elif file_schema[name] is not data_type:
            this_type = file_schema[name]
            schema_exceptions.append(
                f"invalid schema column name {name} type {this_type} is not {data_type}"
            )

    if schema_exceptions:
        msg = "\n".join(schema_exceptions)
        msg = f"invalid format:\n{msg}"
        raise Exception(msg)

    return df


@app.command()
def init(project: str, directory: str = "."):
    """Initialise the project directory with the suggested structure and start config files

    --directory:str - Where you want the project to be initialised
    """
    _init.init(project, directory)

from pprint import pprint

@app.command()
def import_tables(
    location: str,
    format: Annotated[
        ImportFormat, typer.Option(case_sensitive=False)
    ] = ImportFormat.excel,
):
    """Import tables configuration from an external source such as a Excel.

    --location:str - The uri indicator of the table metadata e.g. the file path if importing a csv
    --format:ImportFormat -  The format of the table metadata to import e.g. excel
    """

    df = pd.read_excel(location, header=[0, 1])
    # df = validate_schema(df)
    def rename(col):
        if isinstance(col, tuple):
            col = '.'.join(str(c) for c in col if not c.startswith("Unnamed:"))
        return col
    df.columns = map(rename, df.columns)
    pprint(df.to_dict(orient="records"))



@app.command()
def build():
    """Build assets such as databricks workflows."""
    # TODO: implement workflow build
    pass


if __name__ in ["yetl.__main__", "__main__"]:
    app()

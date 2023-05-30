
from enum import Enum
import pandas as pd
import numpy as np
from ...config import StageType
from pydantic import BaseModel, Field
import yaml
from functools import reduce
import os
from typing import Any, Dict, List, Union

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


# def read_value(row:pd.Series, header_1:ColumnNames, header_2:ColumnNames = None):

#     value = row[header_1]
#     if header_2:
#         value = value[header_2]
    
#     if pd.notna(value):
#         value = value.values[0]
#         return value
#     else:
#         return ""
    
class Metadata(BaseModel):

    SCHEMA = {
        ColumnNames.stage: str,
        ColumnNames.table_type: str,
        ColumnNames.database: str,
        ColumnNames.table: str,
        ColumnNames.sql: str,
        ColumnNames.ids: str,
        ColumnNames.depends_on: str,
        f"{ColumnNames.deltalake}_{ColumnNames.delta_properties}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.identity}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.partition_by}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.delta_constraints}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.z_order_by}": str,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.invalid_ratio}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.invalid_rows}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.max_rows}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.mins_rows}": np.float64,
        f"{ColumnNames.error_thresholds}_{ColumnNames.invalid_ratio}": np.float64,
        f"{ColumnNames.error_thresholds}_{ColumnNames.invalid_rows}": np.float64,
        f"{ColumnNames.error_thresholds}_{ColumnNames.max_rows}": np.float64,
        f"{ColumnNames.error_thresholds}_{ColumnNames.mins_rows}": np.float64,
        # "custom_properties.process_group": np."float64",
        # "custom_properties.rentention_days": np."float64",
        # "custom_properties.vaccum": np."float64"
    }

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)


    stage: StageType = Field(...)
    table_type: str = Field(...)
    database: str = Field(...)
    table: str = Field(...)
    sql: str = Field(default=None)
    ids: str = Field(default=None)
    depends_on: str = Field(default=None)
    deltalake_delta_properties: str = Field(default=None)
    deltalake_identity: str = Field(default=None)
    deltalake_partition_by: str = Field(default=None)
    deltalake_delta_constraints: str = Field(default=None)
    deltalake_z_order_by: str = Field(default=None)
    warning_thresholds_invalid_ratio: float = Field(default=None)
    warning_thresholds_invalid_rows: float = Field(default=None)
    warning_thresholds_max_rows: float = Field(default=None)
    warning_thresholds_mins_rows: float = Field(default=None)
    error_thresholds_invalid_ratio: float = Field(default=None)
    error_thresholds_invalid_rows: float = Field(default=None)
    error_thresholds_max_rows: float = Field(default=None)
    error_thresholds_mins_rows: float = Field(default=None)

    def _get_list(self, data:str):
        if data is None:
            return None
        else:
            datal = [i.strip() for i in data.split("\n")]
            if len(datal) == 1:
                return datal[0]
            else:
                return datal

    def _has_warning_thresholds(self):
        return any([
            self.warning_thresholds_invalid_ratio is not None,
            self.warning_thresholds_invalid_rows is not None,
            self.warning_thresholds_max_rows is not None,
            self.warning_thresholds_mins_rows is not None,
        ])
    
    def _has_error_thresholds(self):
        return any([
            self.error_thresholds_invalid_ratio is not None,
            self.error_thresholds_invalid_rows is not None,
            self.error_thresholds_max_rows is not None,
            self.error_thresholds_mins_rows is not None,
        ])

    def _has_properties(self):
        return any([
            self.sql is not None,
            self.ids is not None,
            self.depends_on is not None,
            self.deltalake_delta_properties is not None,
            self.deltalake_identity is not None,
            self.deltalake_partition_by is not None,
            self.deltalake_delta_constraints is not None,
            self.deltalake_z_order_by is not None,
            self._has_warning_thresholds(),
            self._has_error_thresholds()
        ])

    def _get_error_thresholds(self):

        if self._has_error_thresholds():
            data = {"error_thresholds": {}}
            if self.error_thresholds_invalid_ratio is not None:
                data["error_thresholds"]["invalid_ratio"] = self.error_thresholds_invalid_ratio
            if self.error_thresholds_invalid_rows is not None:
                data["error_thresholds"]["invalid_rows"] = self.error_thresholds_invalid_rows
            if self.error_thresholds_max_rows is not None:
                data["error_thresholds"]["max_rows"] = self.error_thresholds_max_rows
            if self.error_thresholds_mins_rows is not None:
                data["error_thresholds"]["mins_rows"] = self.error_thresholds_mins_rows
            return data
        else:
            return None
        
    def _get_warning_thresholds(self):

        if self._has_warning_thresholds():
            data = {"warning_thresholds": {}}
            if self.warning_thresholds_invalid_ratio is not None:
                data["warning_thresholds"]["invalid_ratio"] = self.warning_thresholds_invalid_ratio
            if self.warning_thresholds_invalid_rows is not None:
                data["warning_thresholds"]["invalid_rows"] = self.warning_thresholds_invalid_rows
            if self.warning_thresholds_max_rows is not None:
                data["warning_thresholds"]["max_rows"] = self.warning_thresholds_max_rows
            if self.warning_thresholds_mins_rows is not None:
                data["warning_thresholds"]["mins_rows"] = self.warning_thresholds_mins_rows
            return data
        else:
            return None

    def dict(self):

        data  = {
            str(self.stage.value) : {
                self.table_type : {
                    self.database: {
                        self.table : None
                    }
                }
            }
        }

        table = None
        if self._has_properties():
            table = {}
            if self.depends_on is not None:
                table["depends_on"] = self._get_list(self.depends_on)
            if self.ids is not None:
                table["ids"] = self._get_list(self.ids)
            if self.sql is not None and self.sql.lower() == "y":
                table["sql"] = "../sql/{{database}}/{{table}}.sql"
            if self.deltalake_partition_by is not None:
                table["partition_by"] = self._get_list(self.deltalake_partition_by)
            if self.deltalake_z_order_by is not None:
                table["z_order_by"] = self._get_list(self.deltalake_z_order_by)
            if self.deltalake_delta_constraints is not None:
                table["delta_constraints"] = self._get_list(self.deltalake_delta_constraints)
            if self.deltalake_delta_properties is not None:
                table["delta_properties"] = self._get_list(self.deltalake_delta_properties)
            if self._has_warning_thresholds():
                table["warning_thresholds"] = self._get_warning_thresholds()["warning_thresholds"]
            if self._has_error_thresholds():
                table["error_thresholds"] = self._get_error_thresholds()["error_thresholds"]


        data[self.stage.value][self.table_type][self.database][self.table] = table
        
        return data

class XlsMetadata(BaseModel):

    SCHEMA = {
        ColumnNames.stage: str,
        ColumnNames.table_type: str,
        ColumnNames.database: str,
        ColumnNames.table: str,
        ColumnNames.sql: str,
        ColumnNames.ids: str,
        ColumnNames.depends_on: str,
        f"{ColumnNames.deltalake}_{ColumnNames.delta_properties}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.identity}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.partition_by}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.delta_constraints}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.z_order_by}": str,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.invalid_ratio}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.invalid_rows}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.max_rows}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.mins_rows}": np.float64,
        f"{ColumnNames.error_thresholds}_{ColumnNames.invalid_ratio}": np.float64,
        f"{ColumnNames.error_thresholds}_{ColumnNames.invalid_rows}": np.float64,
        f"{ColumnNames.error_thresholds}_{ColumnNames.max_rows}": np.float64,
        f"{ColumnNames.error_thresholds}_{ColumnNames.mins_rows}": np.float64,
        # "custom_properties.process_group": np."float64",
        # "custom_properties.rentention_days": np."float64",
        # "custom_properties.vaccum": np."float64"
    }

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        df = pd.read_excel(self.location, header=[0, 1])
        df = self.validate_schema(df)

        df = df.to_dict(orient="records")
        df = [Metadata(
                **{
                    "_".join([n for n in list(k) if not n.startswith("Unnamed")]): (None if pd.isna(v) else v) 
                    for k,v in r.items()
                }
            ).dict() for r in df]
        
        # deep merge
        self.data = reduce(XlsMetadata.merge, [r for r in df])


    location:str = Field(...)
    data:dict = Field(default=None)

    def write(self):
        data = yaml.safe_dump(self.data, indent=2)

        path = self.location.replace(os.path.basename(self.location), "")
        path = os.path.join(path, "tables_test.yaml")

        with open(path, "w", encoding="utf-8") as f:
            f.write(data)

    
    @classmethod
    def merge(cls, a, b, path=None):
        "merges b into a"
        if path is None: path = []
        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    XlsMetadata.merge(a[key], b[key], path + [str(key)])
                elif a[key] == b[key]:
                    pass # same leaf value
                else:
                    raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
            else:
                a[key] = b[key]
        return a

    def validate_schema(self, df: pd.DataFrame):
        schema_exceptions = []
        file_schema = {}
        for column in df:
            if df[column].name[0] != "custom_properties":
                if df[column].dtype.type == np.object_:
                    df[column] = df[column].astype(pd.StringDtype())
                name = [n for n in df[column].name if not n.startswith("Unnamed:")]
                name = "_".join(name)
                file_schema[name] = df[column].dtype.type

        for name, data_type in self.SCHEMA.items():
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

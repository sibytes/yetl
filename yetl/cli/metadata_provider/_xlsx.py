from enum import Enum
import pandas as pd
import numpy as np
from ...config import StageType, TableType
from pydantic import BaseModel, Field
import yaml
from functools import reduce
import os
from typing import Any, Union, Dict
import pkg_resources


class ImportFormat(str, Enum):
    excel = "excel"


class ColumnNames(str, Enum):
    stage = "stage"
    table_type = "table_type"
    catalog = "catalog"
    database = "database"
    table = "table"
    sql = "sql"
    id = "id"
    depends_on = "depends_on"
    deltalake = "deltalake"
    identity = "identity"
    partition_by = "partition_by"
    cluster_by = "cluster_by"
    delta_constraints = "delta_constraints"
    z_order_by = "z_order_by"
    delta_properties = "delta_properties"
    exception_thresholds = "exception_thresholds"
    warning_thresholds = "warning_thresholds"
    invalid_ratio = "invalid_ratio"
    invalid_rows = "invalid_rows"
    max_rows = "max_rows"
    min_rows = "min_rows"
    custom_properties = "custom_properties"
    vacuum = "vacuum"


class Metadata(BaseModel):
    SCHEMA = {
        ColumnNames.stage: str,
        ColumnNames.table_type: str,
        ColumnNames.catalog: str,
        ColumnNames.database: str,
        ColumnNames.table: str,
        ColumnNames.sql: str,
        ColumnNames.id: str,
        ColumnNames.depends_on: str,
        f"{ColumnNames.deltalake}_{ColumnNames.delta_properties}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.identity}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.partition_by}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.cluster_by}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.delta_constraints}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.z_order_by}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.vacuum}": int,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.invalid_ratio}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.invalid_rows}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.max_rows}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.min_rows}": np.float64,
        f"{ColumnNames.exception_thresholds}_{ColumnNames.invalid_ratio}": np.float64,
        f"{ColumnNames.exception_thresholds}_{ColumnNames.invalid_rows}": np.float64,
        f"{ColumnNames.exception_thresholds}_{ColumnNames.max_rows}": np.float64,
        f"{ColumnNames.exception_thresholds}_{ColumnNames.min_rows}": np.float64,
    }

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    stage: StageType = Field(...)
    table_type: TableType = Field(...)
    catalog: str = Field(default=None)
    database: str = Field(...)
    table: str = Field(...)
    sql: str = Field(default=None)
    id: str = Field(default=None)
    depends_on: str = Field(default=None)
    deltalake_delta_properties: str = Field(default=None)
    custom_properties: Dict[str, Any] = Field(default=None)
    deltalake_identity: str = Field(default=None)
    deltalake_partition_by: str = Field(default=None)
    deltalake_cluster_by: str = Field(default=None)
    deltalake_delta_constraints: str = Field(default=None)
    deltalake_z_order_by: str = Field(default=None)
    deltalake_vacuum: Union[int, None] = Field(default=None)
    warning_thresholds_invalid_ratio: float = Field(default=None)
    warning_thresholds_invalid_rows: int = Field(default=None)
    warning_thresholds_max_rows: int = Field(default=None)
    warning_thresholds_min_rows: int = Field(default=None)
    exception_thresholds_invalid_ratio: float = Field(default=None)
    exception_thresholds_invalid_rows: int = Field(default=None)
    exception_thresholds_max_rows: int = Field(default=None)
    exception_thresholds_min_rows: int = Field(default=None)
    version: str = Field(
        default=pkg_resources.get_distribution("yetl-framework").version
    )

    def _get_list(self, data: Any, default_singluar_to_str: bool = True):
        if data is None:
            return None
        elif isinstance(data, str):
            datal = [i.strip() for i in data.split("\n")]
            if len(datal) == 1 and default_singluar_to_str:
                return datal[0]
            else:
                return datal
        else:
            return data

    def _get_dict(self, data: str):
        if data is None:
            return None
        else:
            datad = {
                i.split(":")[0].strip(): i.split(":")[1].strip()
                for i in data.split("\n")
            }
            for k, v in datad.items():
                if v == "true":
                    datad[k] = True
                if v == "false":
                    datad[k] = False
            return datad

    def _has_warning_thresholds(self):
        return any(
            [
                self.warning_thresholds_invalid_ratio is not None,
                self.warning_thresholds_invalid_rows is not None,
                self.warning_thresholds_max_rows is not None,
                self.warning_thresholds_min_rows is not None,
            ]
        )

    def _has_exception_thresholds(self):
        return any(
            [
                self.exception_thresholds_invalid_ratio is not None,
                self.exception_thresholds_invalid_rows is not None,
                self.exception_thresholds_max_rows is not None,
                self.exception_thresholds_min_rows is not None,
            ]
        )

    def _has_properties(self):
        return any(
            [
                self.sql is not None,
                self.id is not None,
                self.depends_on is not None,
                self.deltalake_delta_properties is not None,
                self.deltalake_identity is not None,
                self.deltalake_partition_by is not None,
                self.deltalake_cluster_by is not None,
                self.deltalake_delta_constraints is not None,
                self.deltalake_z_order_by is not None,
                self._has_warning_thresholds(),
                self._has_exception_thresholds(),
            ]
        )

    def _get_exception_thresholds(self):
        if self._has_exception_thresholds():
            data = {ColumnNames.exception_thresholds.name: {}}
            if self.exception_thresholds_invalid_ratio is not None:
                data[ColumnNames.exception_thresholds.name][
                    ColumnNames.invalid_ratio.name
                ] = self.exception_thresholds_invalid_ratio

            if self.exception_thresholds_invalid_rows is not None:
                data[ColumnNames.exception_thresholds.name][
                    ColumnNames.invalid_rows.name
                ] = self.exception_thresholds_invalid_rows

            if self.exception_thresholds_max_rows is not None:
                data[ColumnNames.exception_thresholds.name][
                    ColumnNames.max_rows.name
                ] = self.exception_thresholds_max_rows

            if self.exception_thresholds_min_rows is not None:
                data[ColumnNames.exception_thresholds.name][
                    ColumnNames.min_rows.name
                ] = self.exception_thresholds_min_rows
            return data
        else:
            return None

    def _get_warning_thresholds(self):
        if self._has_warning_thresholds():
            data = {ColumnNames.warning_thresholds.name: {}}
            if self.warning_thresholds_invalid_ratio is not None:
                data[ColumnNames.warning_thresholds.name][
                    ColumnNames.invalid_ratio.name
                ] = self.warning_thresholds_invalid_ratio

            if self.warning_thresholds_invalid_rows is not None:
                data[ColumnNames.warning_thresholds.name][
                    ColumnNames.invalid_rows.name
                ] = self.warning_thresholds_invalid_rows

            if self.warning_thresholds_max_rows is not None:
                data[ColumnNames.warning_thresholds.name][
                    ColumnNames.max_rows.name
                ] = self.warning_thresholds_max_rows

            if self.warning_thresholds_min_rows is not None:
                data[ColumnNames.warning_thresholds.name][
                    ColumnNames.min_rows.name
                ] = self.warning_thresholds_min_rows

            return data
        else:
            return None

    def dict(self):
        data = {
            str(self.stage.value): {
                self.table_type.value: {self.database: {self.table: None}}
            }
        }

        table = None
        if self._has_properties():
            table = {}
            if self.depends_on is not None:
                table[ColumnNames.depends_on.name] = self._get_list(
                    self.depends_on, default_singluar_to_str=False
                )
            if self.id is not None:
                table[ColumnNames.id.name] = self._get_list(self.id)
            if self.sql is not None and self.sql.lower() == "y":
                table["sql"] = "../sql/{{database}}/{{table}}.sql"
            if self.deltalake_partition_by is not None:
                table[ColumnNames.partition_by.name] = self._get_list(
                    self.deltalake_partition_by
                )
            if self.deltalake_cluster_by is not None:
                table[ColumnNames.cluster_by.name] = self._get_list(
                    self.deltalake_cluster_by
                )
            if self.deltalake_z_order_by is not None:
                table[ColumnNames.z_order_by.name] = self._get_list(
                    self.deltalake_z_order_by
                )
            if self.deltalake_vacuum is not None:
                table[ColumnNames.vacuum.name] = self._get_list(self.deltalake_vacuum)
            if self.deltalake_delta_properties is not None:
                table[ColumnNames.delta_properties.name] = self._get_dict(
                    self.deltalake_delta_properties
                )
            if self.custom_properties is not None:
                table[ColumnNames.custom_properties.name] = self.custom_properties
            if self._has_warning_thresholds():
                table[
                    ColumnNames.warning_thresholds.name
                ] = self._get_warning_thresholds()[ColumnNames.warning_thresholds.name]
            if self._has_exception_thresholds():
                table[
                    ColumnNames.exception_thresholds.name
                ] = self._get_exception_thresholds()[
                    ColumnNames.exception_thresholds.name
                ]
        data["version"] = self.version
        data[self.stage.value][self.table_type.value][self.database][
            ColumnNames.catalog.name
        ] = self.catalog
        data[self.stage.value][self.table_type.value][self.database][self.table] = table

        return data


class XlsMetadata(BaseModel):
    SCHEMA = {
        ColumnNames.stage: str,
        ColumnNames.table_type: str,
        ColumnNames.catalog: str,
        ColumnNames.database: str,
        ColumnNames.table: str,
        ColumnNames.sql: str,
        ColumnNames.id: str,
        ColumnNames.depends_on: str,
        f"{ColumnNames.deltalake}_{ColumnNames.delta_properties}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.identity}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.partition_by}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.cluster_by}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.delta_constraints}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.z_order_by}": str,
        f"{ColumnNames.deltalake}_{ColumnNames.vacuum}": str,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.invalid_ratio}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.invalid_rows}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.max_rows}": np.float64,
        f"{ColumnNames.warning_thresholds}_{ColumnNames.min_rows}": np.float64,
        f"{ColumnNames.exception_thresholds}_{ColumnNames.invalid_ratio}": np.float64,
        f"{ColumnNames.exception_thresholds}_{ColumnNames.invalid_rows}": np.float64,
        f"{ColumnNames.exception_thresholds}_{ColumnNames.max_rows}": np.float64,
        f"{ColumnNames.exception_thresholds}_{ColumnNames.min_rows}": np.float64,
        # "custom_properties.process_group": np."float64",
        # "custom_properties.rentention_days": np."float64",
        # "custom_properties.vaccum": np."float64"
    }

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        df = pd.read_excel(self.source, header=[0, 1])
        df = self.validate_schema(df)
        self.data = self._deserialize(df)

    source: str = Field(...)
    data: dict = Field(default=None)

    def _auto_convert(self, data: Any):
        if isinstance(data, float):
            if data.is_integer():
                return int(data)
        if isinstance(data, str):
            try:
                data = float(data)
            except ValueError:
                pass

            try:
                data = int(data)
            except ValueError:
                pass

            if data.lower() in ["true", "false"]:
                data = bool(data)

            return data
        else:
            return data

    def _deserialize(self, df: pd.DataFrame):
        df = df.to_dict(orient="records")
        metadata = [
            {
                "_".join([n for n in list(k) if not n.startswith("Unnamed")]): (
                    None if pd.isna(v) else v
                )
                for k, v in r.items()
            }
            for r in df
        ]
        for item in metadata:
            custom_properties = {}
            for k, v in item.items():
                if k.startswith(ColumnNames.custom_properties.name) and v is not None:
                    prop_key = k.replace(f"{ColumnNames.custom_properties.name}_", "")
                    custom_properties[prop_key] = self._auto_convert(v)
            if custom_properties:
                item[ColumnNames.custom_properties.name] = custom_properties

        df = [Metadata(**m).dict() for m in metadata]

        # deep merge
        data = reduce(XlsMetadata.merge, [r for r in df])

        return data

    def write(self, path: str = None):
        if path is None:
            path = self.source.replace(os.path.basename(self.source), "")
            path = os.path.join(path, "tables.yaml")

        with open(path, "w", encoding="utf-8") as f:
            f.write(
                "# yaml-language-server: $schema=./json_schema/sibytes_yetl_tables_schema.json\n\n"
            )

            version = self.data["version"]
            f.write(f"version: {version}\n")
            f.write("\n")
            for t in StageType:
                stage_data = {t.name: self.data.get(t.name)}
                if stage_data[t.name] is not None:
                    stage_data = yaml.safe_dump(stage_data, indent=2)
                    f.write(stage_data)
                    f.write("\n")

    @classmethod
    def merge(cls, data_1: dict, data_2: dict, path=None):
        if path is None:
            path = []
        for key in data_2:
            if key in data_1:
                if isinstance(data_1[key], dict) and isinstance(data_2[key], dict):
                    XlsMetadata.merge(data_1[key], data_2[key], path + [str(key)])
                elif data_1[key] == data_2[key]:
                    pass  # same leaf value
                else:
                    raise Exception("Conflict at %s" % ".".join(path + [str(key)]))
            else:
                data_1[key] = data_2[key]
        return data_1

    def validate_schema(self, df: pd.DataFrame, validate_type: bool = False):
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

            if validate_type and file_schema[name] is not data_type:
                this_type = file_schema[name]
                schema_exceptions.append(
                    f"invalid schema column name {name} type {this_type} is not {data_type}"
                )

        if schema_exceptions:
            msg = "\n".join(schema_exceptions)
            msg = f"invalid format:\n{msg}"
            raise Exception(msg)

        return df

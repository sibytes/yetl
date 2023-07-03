from pydantic import BaseModel, Field, PrivateAttr
from typing import Union, Any, Dict, List
from ._stage_type import StageType
import fnmatch
from ._table_mapping import TableMapping
from .table import TableType
from .table import table_factory
from .table import Table
from enum import Enum
import logging

_INDEX_WILDCARD = "*"


class KeyContants(Enum):
    DATABASE = "database"
    TABLE = "table"
    TABLES = "tables"
    STAGE = "stage"
    TABLE_TYPE = "table_type"
    PROJECT = "project"
    TIMESLICE = "timeslice"
    CONFIG_PATH = "config_path"


class PushDownProperties(Enum):
    DELTA_PROPETIES = "delta_properties"
    CATALOG = "catalog"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_

    @classmethod
    def has_not_value(cls, value):
        return value not in cls._value2member_map_


class Tables(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._parse_configuration()
        self._build_tables()

    def _parse_configuration(self):
        push_down_properties = {}
        for stage_name, table_type in self.table_data["tables"].items():
            stage_type = StageType(stage_name)
            for table_type_name, database in table_type.items():
                table_type = TableType(table_type_name)
                push_down_properties = {}
                for database_name, table in database.items():
                    if PushDownProperties.has_not_value(database_name):
                        catalog = table.get(PushDownProperties.CATALOG.value)
                        del table[PushDownProperties.CATALOG.value]
                        for table_name, table_properties in table.items():
                            table_config = {
                                KeyContants.DATABASE.value: database_name,
                                KeyContants.TABLE.value: table_name,
                                KeyContants.STAGE.value: stage_type,
                                KeyContants.TABLE_TYPE.value: table_type,
                                KeyContants.PROJECT.value: self.table_data.get(
                                    KeyContants.PROJECT.value
                                ),
                                KeyContants.TIMESLICE.value: self.table_data.get(
                                    KeyContants.TIMESLICE.value
                                ),
                                KeyContants.CONFIG_PATH.value: self.table_data.get(
                                    KeyContants.CONFIG_PATH.value
                                ),
                            }
                            if table_properties:
                                table_config = {**table_config, **table_properties}
                            table_config = {**push_down_properties, **table_config}
                            table_config[PushDownProperties.CATALOG.value] = catalog
                            for p, v in push_down_properties.items():
                                if isinstance(v, dict) and table_config.get(p):
                                    table_config[p] = {**v, **table_config[p]}
                                else:
                                    table_config[p] = v
                            stage_config = self.table_data.get(stage_type.value, {})
                            stage_config = stage_config.get(table_type.value, {})
                            table_config = {**stage_config, **table_config}
                            index = f"{stage_name}.{database_name}.{table_name}"
                            self.tables_index[index] = table_config
                    else:
                        push_down_properties[database_name] = table

    table_data: dict = Field(...)
    tables_index: Dict[str, Table] = Field(default={})
    delta_properties: Dict[str, str] = Field(default=None)
    _logger: Any = PrivateAttr(default=None)

    @classmethod
    def get_index(
        cls,
        stage: Union[StageType, str] = _INDEX_WILDCARD,
        database=_INDEX_WILDCARD,
        table=_INDEX_WILDCARD,
    ):
        if isinstance(stage, StageType):
            return f"{stage.name}.{database}.{table}"
        else:
            return f"{stage}.{database}.{table}"

    @classmethod
    def parse_index(
        cls,
        index: str,
    ):
        try:
            parts = index.split(".")
            stage = StageType[parts[0]]
            database = parts[1]
            table = parts[2]
        except Exception as e:
            raise Exception(
                f"attempted to parse an invalid index {index}. It must be of the form 'stage.database.table'"
            ) from e

        return stage, database, table

    def _build_tables(self):
        """
        Parse through the table definitions dictionary and deserialize it
        into Table objects. The table object are then place in a dictionary for easy
        lookup with a key = stage.database.table and the value being the table
        object it self. This dictionary index is held on self.tables_index
        """
        for index, table_config in self.tables_index.items():
            self.tables_index[index] = table_factory.make(
                table_config["table_type"], table_config
            )

    def create_table(
        self,
        stage: Union[StageType, str] = _INDEX_WILDCARD,
        database=_INDEX_WILDCARD,
        table=_INDEX_WILDCARD,
        first_match: bool = True,
        catalog: str = None,
        catalog_enabled: bool = True,
        **kwargs,
    ):
        return self.lookup_table(
            stage=stage,
            database=database,
            table=table,
            first_match=first_match,
            create_database=True,
            create_table=True,
            catalog=catalog,
            catalog_enabled=catalog_enabled,
            **kwargs,
        )

    def lookup_table(
        self,
        stage: Union[StageType, str] = _INDEX_WILDCARD,
        database=_INDEX_WILDCARD,
        table=_INDEX_WILDCARD,
        first_match: bool = True,
        create_database: bool = False,
        create_table: bool = False,
        catalog: str = None,
        catalog_enabled: bool = True,
        **kwargs,
    ):
        index = Tables.get_index(stage, database, table)
        matches = fnmatch.filter(list(self.tables_index.keys()), index)

        if not matches:
            raise Exception(f"index {index} not found in tables_index")

        def match_property(
            table: Table, properties: Dict[str, Any], matches: List[str]
        ):
            for p, v in properties.items():
                if (
                    isinstance(table.custom_properties, dict)
                    and table.custom_properties.get(p) == v
                ):
                    return True
                else:
                    index = Tables.get_index(table.stage, table.database, table.table)
                    if index in matches:
                        matches.remove(
                            Tables.get_index(table.stage, table.database, table.table)
                        )
                    return False

        tables_index = dict(self.tables_index)
        if kwargs:
            tables_index = {
                k: v
                for k, v in self.tables_index.items()
                if match_property(v, kwargs, matches)
            }

        if first_match:
            matches = matches[0]
            table = tables_index[matches]
            msg_tables = f"{table.database}.{table.table}"
            self._logger.info(f"Matched tables: {msg_tables}")
            if create_database:
                table.create_database(catalog=catalog, catalog_enabled=catalog_enabled)
            if create_table:
                table.create_table(catalog=catalog, catalog_enabled=catalog_enabled)
            return table
        else:
            tables = [tables_index[i] for i in matches]
            msg_tables = "\n".join([f"{t.database}.{t.table}" for t in tables])
            self._logger.info(f"Matched tables: {msg_tables}")
            db = ""
            if create_table or create_database:
                for t in tables:
                    if create_database and db != t.database:
                        db = t.database
                        t.create_database(
                            catalog=catalog, catalog_enabled=catalog_enabled
                        )
                    if create_table:
                        t.create_table()
            return tables

    def get_table_mapping(
        self,
        stage: StageType,
        table=_INDEX_WILDCARD,
        database=_INDEX_WILDCARD,
        create_database: bool = True,
        create_table: bool = True,
        catalog: str = None,
        catalog_enabled: bool = True,
    ):
        destination = self.lookup_table(
            stage=stage,
            database=database,
            table=table,
            first_match=True,
            create_database=create_database,
            create_table=create_table,
            catalog=catalog,
            catalog_enabled=catalog_enabled,
        )
        source = {}

        tables = []
        try:
            for index in destination.depends_on:
                do_stage, do_database, do_table = Tables.parse_index(index)
                tables = tables + self.lookup_table(
                    stage=do_stage,
                    table=do_table,
                    database=do_database,
                    first_match=False,
                    create_database=create_database,
                    create_table=create_table,
                    catalog=catalog,
                    catalog_enabled=catalog_enabled,
                )
        except Exception as e:
            raise Exception(f"Error looking up dependencies for table {table}") from e

        for tbl in tables:
            source[tbl.table] = tbl

        if len(list(source.values())) == 1:
            source = list(source.values())[0]

        return TableMapping(source=source, destination=destination)

from pydantic import BaseModel, Field
from typing import Union, Any, Dict, List
from ._stage_type import StageType
import fnmatch
from ._table_mapping import TableMapping
from .dataset.dataset_type import TableType
from .dataset import dataset_factory
from .dataset._dataset import Table


_INDEX_WILDCARD = "*"


class Tables(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._load_index()

    table_data: dict = Field(...)
    tables_index: Dict[str, Table] = Field(default={})
    delta_properties: Dict[str, str] = Field(default=None)
    config_path: str = Field(...)

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

    def _parse_table_delta_lake_properties(
        self,
        table_type: TableType,
        table_details: dict,
        merge_properties: dict = {},
    ):
        """
        Parse the delta properties from the tables details if there are any. Merge the into
        a set of existing properties with the table detail properties taking precedence.
        """
        if table_type is TableType.DeltaLakeTable:
            delta_properties = table_details.get("delta_properties", {})
            # delta_properties = stage_delta_properties | delta_properties
            delta_properties = {
                **merge_properties,
                **delta_properties,
            }
            return delta_properties
        else:
            return None

    def _parse_stage_delta_lake_properties(
        self, table_type: TableType, database_tables: dict
    ):
        """Parse delta lake properties from the database table details and remove it"""
        stage_delta_properties = None
        if table_type is TableType.DeltaLakeTable:
            stage_delta_properties = database_tables.get("delta_properties", {})
            if stage_delta_properties:
                del database_tables["delta_properties"]
        return stage_delta_properties

    def _parse_stage_data(self, stage: StageType, stage_data: dict):
        """
        loop through the database tables, create the table
        and add it to the class index.
        """
        for table_type, database_tables in stage_data.items():
            table_type = TableType(table_type)
            stage_delta_properties = self._parse_stage_delta_lake_properties(
                table_type, database_tables
            )
            for database, tables in database_tables.items():
                for table, table_details in tables.items():
                    # flatten the config structure for a table
                    if not table_details:
                        table_details = {}

                    delta_properties = self._parse_table_delta_lake_properties(
                        table_type, table_details, stage_delta_properties
                    )
                    if delta_properties:
                        table_details["delta_properties"] = delta_properties
                    table_details["table"] = table
                    table_details["database"] = database
                    table_details["stage"] = stage
                    table_details["table_type"] = table_type

                    # create a table object

                    # table = Table(**table_details)
                    table = dataset_factory.get_table(table_type, table_details)

                    # index the table object
                    index = f"{stage.name}.{database}.{table.table}"
                    self.tables_index[index] = table

    def _load_index(self):
        """
        Parse through the table definitions dictionary and deserialize it
        into Table objects. The table object are then place in a dictionary for easy
        lookup with a key = stage.database.table and the value being the table
        object it self. This dictionary index is held on self.tables_index
        """
        for stage in StageType:
            stage_data = self.table_data.get(stage)
            if stage_data:
                self._parse_stage_data(stage, stage_data)

    def lookup_table(
        self,
        stage: Union[StageType, str] = _INDEX_WILDCARD,
        database=_INDEX_WILDCARD,
        table=_INDEX_WILDCARD,
        first_match: bool = True,
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
                    index = Tables.get_index(table.stage, table.database, table.name)
                    if index in matches:
                        matches.remove(
                            Tables.get_index(table.stage, table.database, table.name)
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
            return table
        else:
            tables = [tables_index[i] for i in matches]
            return tables

    def get_table_mapping(
        self, stage: StageType, table=_INDEX_WILDCARD, database=_INDEX_WILDCARD
    ):
        destination = self.lookup_table(
            stage=stage, database=database, table=table, first_match=True
        )
        source = {}

        try:
            for index in destination.depends_on:
                do_stage, do_database, do_table = Tables.parse_index(index)
                table = self.lookup_table(
                    stage=do_stage,
                    table=do_table,
                    database=do_database,
                    first_match=True,
                )
                source[table.table] = table
        except Exception as e:
            raise Exception(f"Error looking up dependencies for table {table}") from e

        if len(list(source.values())) == 1:
            source = list(source.values())[0]

        return TableMapping(source=source, destination=destination)

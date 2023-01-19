from enum import Enum


from ._spark_file_schema_repo import SparkFileSchemaRepo
from ._deltalake_sql_file import DeltalakeSchemaFile

# from ._sql_reader_file import SqlReaderFile
from ._i_schema_repo import ISchemaRepo
import logging


class SchemaRepoType(Enum):
    SPARK_SCHEMA_FILE = "spark_schema_file"
    DELTALAKE_SQL_FILE = "deltalake_sql_file"
    PIPELINE_FILE = "sql_root"


class _SchemaRepoFactory:
    def __init__(self) -> None:
        self._schema_repo = {}
        self._logger = logging.getLogger(self.__class__.__name__)

    def register_schema_repo_type(
        self, sr_type: SchemaRepoType, schema_repo_type: type
    ):
        self._logger.debug(f"Register file system type {schema_repo_type} as {type}")
        self._schema_repo[sr_type] = schema_repo_type

    def get_schema_repo_type(self, config: dict) -> ISchemaRepo:

        schema_repo_store: str = next(iter(config))
        sr_type: SchemaRepoType = SchemaRepoType(schema_repo_store)

        self._logger.debug(f"Setting up schema repo on {schema_repo_store} ")

        self._logger.debug(f"Setting SchemaRepoType using type {sr_type}")
        schema_repo: ISchemaRepo = self._schema_repo.get(sr_type)

        if not schema_repo:
            self._logger.error(
                f"SchemaRepoType {sr_type.name} not registered in the schema_repo factory"
            )
            raise ValueError(sr_type)

        config = config[schema_repo_store]
        return schema_repo(**config)


factory = _SchemaRepoFactory()
factory.register_schema_repo_type(SchemaRepoType.SPARK_SCHEMA_FILE, SparkFileSchemaRepo)
factory.register_schema_repo_type(
    SchemaRepoType.DELTALAKE_SQL_FILE, DeltalakeSchemaFile
)
# factory.register_schema_repo_type(SchemaRepoType.PIPELINE_FILE, SqlReaderFile)

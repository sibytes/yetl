from enum import Enum


from ._spark_file_schema_repo import SparkFileSchemaRepo
from ._deltalake_sql_file import DeltalakeSchemaFile
from ._ischema_repo import ISchemaRepo
import logging


class SchemaRepoType(Enum):
    SPARK_SCHEMA_FILE = 1
    DELTALAKE_SQL_FILE = 2


class _SchemaRepoFactory:
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)
        self._schema_repo = {}

    def register_schema_repo_type(
        self, sr_type: SchemaRepoType, schema_repo_type: type
    ):
        self._logger.debug(f"Register file system type {schema_repo_type} as {type}")
        self._schema_repo[sr_type] = schema_repo_type

    def _get_sr_type(self, name: str):
        name = name.strip().upper()
        try:
            if SchemaRepoType[name] in SchemaRepoType:
                return SchemaRepoType[name]
        except:
            return None

    def get_schema_repo_type(self, context, config: dict) -> ISchemaRepo:

        schema_repo_store: str = next(iter(config))
        sr_type: SchemaRepoType = self._get_sr_type(schema_repo_store)

        context.log.info(f"Setting up schema repo on {schema_repo_store} ")

        context.log.debug(f"Setting SchemaRepoType using type {sr_type}")
        schema_repo: ISchemaRepo = self._schema_repo.get(sr_type)

        if not schema_repo:
            self._logger.error(
                f"SchemaRepoType {sr_type.name} not registered in the schema_repo factory"
            )
            raise ValueError(sr_type)

        return schema_repo(context, config)


factory = _SchemaRepoFactory()
factory.register_schema_repo_type(SchemaRepoType.SPARK_SCHEMA_FILE, SparkFileSchemaRepo)
factory.register_schema_repo_type(
    SchemaRepoType.DELTALAKE_SQL_FILE, DeltalakeSchemaFile
)

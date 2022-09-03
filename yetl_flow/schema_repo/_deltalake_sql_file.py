import json
from ._ischema_repo import ISchemaRepo
from pyspark.sql.types import StructType
from ..file_system import FileFormat, IFileSystem, file_system_factory, FileSystemType


class DeltalakeSchemaFile(ISchemaRepo):

    _SCHEMA_ROOT = "./config/schema/deltalake"
    _EXT = "sql"

    def __init__(self, context, config: dict) -> None:
        super().__init__(context, config)
        self.root_path = config["deltalake_sql_file"].get("deltalake_schema_root")

    def _mkpath(self, database_name: str, table_name: str):
        """Function that builds the schema path"""
        if not self.root_path:
            return f"{self._SCHEMA_ROOT}/{database_name}/{table_name}.{self._EXT}"
        else:
            return f"{self.root_path}/{database_name}/{table_name}.{self._EXT}"

    def save_schema(self, schema: StructType, database_name: str, table_name: str):
        """Serialise delta table to a create table sql file."""
        raise NotImplementedError

    def load_schema(self, database_name: str, table_name: str):
        """Loads a spark from a yaml file and deserialises to a spark schema."""

        path = self._mkpath(database_name, table_name)

        # this was in thought that schema's could be maintain and loaded off DBFS for databricks
        # it actually works much better using the local repo files
        # maybe considered for a future use case - we need more configuration to set the schema store
        # type, since it's explicitly hand in hand with databricks spark env.
        # fs: IFileSystem = self.context.fs

        # file system is working fine for databricks and vanilla spark deployments.
        fs: IFileSystem = file_system_factory.get_file_system_type(
            self.context, FileSystemType.FILE
        )

        self.context.log.info(
            f"Loading schema for dataset {database_name}.{table_name} from {path} using {type(fs)}"
        )

        schema = fs.read_file(path, FileFormat.TEXT)
        if not schema:
            msg = f"Failed to load schema for dataset {database_name}.{table_name} from {path}"
            raise Exception(msg)

        msg = json.dumps(schema)
        self.context.log.debug(msg)

        return schema

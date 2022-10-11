import json
import yaml
from ._ischema_repo import ISchemaRepo
from pyspark.sql.types import StructType
from ..file_system import FileFormat, IFileSystem, file_system_factory, FileSystemType
import os


class SchemaNotFound(Exception):
    def __init__(self, path: str):
        self.path = path
        super().__init__(self.path)


class SparkFileSchemaRepo(ISchemaRepo):

    _SCHEMA_ROOT = "./config/schema/spark"
    _EXT = "yaml"

    def __init__(self, context, config: dict) -> None:
        super().__init__(context, config)
        self.root_path = config["spark_schema_file"].get(
            "spark_schema_root", self._SCHEMA_ROOT
        )

    def _mkpath(self, database_name: str, table_name: str, sub_location: str):
        """Function that builds the schema path"""
        if sub_location:
            return f"{self.root_path}/{sub_location}/{database_name}/{table_name}.{self._EXT}"
        else:
            return f"{self.root_path}/{database_name}/{table_name}.{self._EXT}"

    def save_schema(
        self,
        schema: StructType,
        database_name: str,
        table_name: str,
        sub_location: str = None,
    ):
        """Serialise a spark schema to a yaml file and saves to a schema file in the schema folder."""

        path = self._mkpath(database_name, table_name, sub_location)
        path = os.path.abspath(path)

        schema_dict = json.loads(schema.json())
        with open(path, "w") as f:
            f.write(yaml.safe_dump(schema_dict))

    def load_schema(
        self, database_name: str, table_name: str, sub_location: str = None
    ):
        """Loads a spark from a yaml file and deserialises to a spark schema."""

        path = self._mkpath(database_name, table_name, sub_location)
        path = os.path.abspath(path)

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
        try:
            schema = fs.read_file(path, FileFormat.YAML)
        except Exception as e:
            msg = f"Failed to load schema for dataset {database_name}.{table_name} from {path}"
            raise SchemaNotFound(path) from e

        msg = json.dumps(schema, indent=4, default=str)
        self.context.log.debug(msg)

        try:
            spark_schema = StructType.fromJson(schema)
        except Exception as e:
            msg = (
                msg
            ) = f"Failed to deserialise spark schema to StructType for dataset {database_name}.{table_name} from {path}"
            raise Exception(msg) from e

        return spark_schema

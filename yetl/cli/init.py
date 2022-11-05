import os
import yaml

_CONFIG_DIR = "config_dev"
_LOG_FILE = """
version: 1
formatters:
  default:
    format: "%(levelname)s : %(asctime)s : %(name)s : %(filename)s.%(funcName)s: line(%(lineno)s) : %(message)s"
handlers:
  console:
    class: logging.StreamHandler
    formatter: default
    stream: ext://sys.stdout
root:
  level: ERROR
  handlers: [console]
"""

_ENV_DBX = f"""
datalake: "/mnt/datalake/yetl_data"
datalake_protocol: "dbfs:"
spark:
  logging_level: ERROR
  config:
    spark.master: local
    spark.databricks.delta.allowArbitraryProperties.enabled: true

spark_schema_repo:
  spark_schema_file:
    spark_schema_root: ./config/schema/spark

deltalake_schema_repo:
  deltalake_sql_file:
    deltalake_schema_root: ./config/schema

# used to write data lineage to
metadata_repo:
  metadata_file:
    metadata_root: /yetl/runs
    metadata_dataset: dataset.json
    metadata_index: index.json

"""

_ENV_LOCAL = """
datalake: "{{cwd}}/data"
datalake_protocol: "file:"
spark:
  logging_level: ERROR
  config:
    spark.master: local
    # yetl uses table properties so this must be set as a table
    # property or globally like here in the spark context
    spark.databricks.delta.allowArbitraryProperties.enabled: true
    spark.jars.packages: io.delta:delta-core_2.12:2.1.0
    park.sql.extensions: io.delta.sql.DeltaSparkSessionExtension
    spark.sql.catalog.spark_catalog: org.apache.spark.sql.delta.catalog.DeltaCatalog
    spark.databricks.delta.merge.repartitionBeforeWrite.enabled: true

spark_schema_repo:
  spark_schema_file:
    spark_schema_root: ./config/schema/spark

deltalake_schema_repo:
  deltalake_sql_file:
    deltalake_schema_root: ./config/schema


# used to write data lineage to
metadata_repo:
  metadata_file:
    metadata_root: ./config/runs
    metadata_dataset: dataset.json
    metadata_index: index.json
"""

_DOT_ENV = """
YETL_ROOT=./config
YETL_ENVIRONMENT=local"""


def _make_config_file(directory:str, filename:str, data:str):
    filepath = os.path.join(directory, filename)
    if not os.path.exists(filepath):
        with open(filepath, mode="w", encoding="utf-8") as f:
            f.write(data)

def init(project:str, home_dir:str="."):

    _make_config_file(home_dir, ".env", _DOT_ENV)

    root_dir = os.path.join(home_dir, _CONFIG_DIR)
    root_dir = os.path.abspath(root_dir)
    os.makedirs(root_dir, exist_ok=True)

    _make_config_file(root_dir, "logging.yaml", _LOG_FILE)

    project_pipeline_path = os.path.join(root_dir, project, "pipelines")
    os.makedirs(project_pipeline_path, exist_ok=True)

    project_path = os.path.join(root_dir, "project", project)
    os.makedirs(project_path, exist_ok=True)

    environment_path = os.path.join(root_dir, "environment")
    os.makedirs(environment_path, exist_ok=True)

    _make_config_file(environment_path, "dbx_dev.yaml", _ENV_DBX)
    _make_config_file(environment_path, "local.yaml", _ENV_LOCAL)

    schema_dirs = ["spark", "deltalake", "sql"]
    for sd in schema_dirs:
        schema_path = os.path.join(root_dir, "schema", sd)
        os.makedirs(schema_path, exist_ok=True)
            


    
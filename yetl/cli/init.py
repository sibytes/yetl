import os
from enum import Enum
import re

_CONFIG_DIR = "config"

def _strip_margin(text):

    return re.sub('\n[ \t]*\|', '\n', text)


def get_log_configuration():
  env_config = _strip_margin(
 f"""version: 1
    |formatters:
    |  default:
    |    format: "%(levelname)s : %(asctime)s : %(name)s : %(filename)s.%(funcName)s: line(%(lineno)s) : %(message)s"
    |handlers:
    |  console:
    |    class: logging.StreamHandler
    |    formatter: default
    |    stream: ext://sys.stdout
    |root:
    |  level: ERROR
    |  handlers: [console]
    """)

  return env_config

def get_dbx_environment_config(config_dir:str = _CONFIG_DIR):
    env_config = _strip_margin(
    """datalake: "/mnt/datalake/yetl_data"
      |datalake_protocol: "dbfs:"
      |spark:
      |  logging_level: ERROR
      |  config:
      |    spark.master: local
      |    spark.databricks.delta.allowArbitraryProperties.enabled: true
      |
      |pipeline_repo:
      |  pipeline_file:
      |    pipeline_root: "./{config}/{{project}}/pipelines"
      |    pipeline_root: "./{config}/{{project}}/sql"
      |
      |spark_schema_repo:
      |  spark_schema_file:
      |    spark_schema_root: ./{config}/schema/spark
      |
      |deltalake_schema_repo:
      |  deltalake_sql_file:
      |    deltalake_schema_root: ./{config}/schema
    """.format(config=config_dir))

    return env_config


def get_local_environment_config(config_dir:str = _CONFIG_DIR):
    env_config = _strip_margin(
    """datalake: "{{cwd}}/data"
      |datalake_protocol: "file:"
      |spark:
      |  logging_level: ERROR
      |  config:
      |    spark.master: local
      |    # yetl uses table properties so this must be set as a table
      |    # property or globally like here in the spark context
      |    spark.databricks.delta.allowArbitraryProperties.enabled: true
      |    spark.jars.packages: io.delta:delta-core_2.12:2.1.0
      |    park.sql.extensions: io.delta.sql.DeltaSparkSessionExtension
      |    spark.sql.catalog.spark_catalog: org.apache.spark.sql.delta.catalog.DeltaCatalog
      |    spark.databricks.delta.merge.repartitionBeforeWrite.enabled: true
      |
      |pipeline_repo:
      |  pipeline_file:
      |    pipeline_root: "./{config}/{{project}}/pipelines"
      |    pipeline_root: "./{config}/{{project}}/sql"
      |
      |spark_schema_repo:
      |  spark_schema_file:
      |    spark_schema_root: ./{config}/schema/spark
      |
      |deltalake_schema_repo:
      |  deltalake_sql_file:
      |    deltalake_schema_root: ./{config}/schema/deltalake
    """.format(config=config_dir))

    return env_config


def _get_dot_env_config(config_dir:str = _CONFIG_DIR):
  env_config = (''
    f'YETL_ROOT=./{config_dir}'
    f'YETL_ENVIRONMENT=local')
  return env_config


def _make_config_file(directory:str, filename:str, data:str, overwrite:bool=False):
    filepath = os.path.join(directory, filename)
    if not os.path.exists(filepath) or overwrite:
        with open(filepath, mode="w", encoding="utf-8") as f:
            f.write(data)

def init(project:str, home_dir:str=".", config_folder:str=_CONFIG_DIR, overwrite:bool = False):

    _make_config_file(home_dir, ".env", _get_dot_env_config())

    root_dir = os.path.join(home_dir, _CONFIG_DIR)
    root_dir = os.path.abspath(root_dir)
    os.makedirs(root_dir, exist_ok=True)

    _make_config_file(root_dir, "logging.yaml", get_log_configuration(), overwrite)

    project_pipeline_path = os.path.join(root_dir, project, "pipelines")
    os.makedirs(project_pipeline_path, exist_ok=True)

    project_path = os.path.join(root_dir, "project", project)
    os.makedirs(project_path, exist_ok=True)

    environment_path = os.path.join(root_dir, "environment")
    os.makedirs(environment_path, exist_ok=True)

    _make_config_file(environment_path, "dbx_dev.yaml", get_dbx_environment_config(config_folder), overwrite)
    _make_config_file(environment_path, "local.yaml", get_local_environment_config(config_folder), overwrite)

    schema_dirs = ["spark", "deltalake", "sql"]
    for sd in schema_dirs:
        schema_path = os.path.join(root_dir, "schema", sd)
        os.makedirs(schema_path, exist_ok=True)
            


    
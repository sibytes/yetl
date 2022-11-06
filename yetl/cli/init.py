import os
import re
import yaml
from enum import Enum
import jinja2

_CONFIG_DIR = "config"
_DBX_CONFIG_FILE = "dbx_dev.yaml"
_LOCAL_CONFIG_FILE = "local.yaml"

class SchemaRepo(Enum):
  SPARK_SCHEMA_REPO = "spark_schema_file"
  DELTALAKE_SCHEMA_REPO = "deltalake_sql_file"
  PIPELINE_REPO = "pipeline_file"


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
      |    pipeline_root: "./{config}/{{{{project}}}}/pipelines"
      |    sql_root: "./{config}/{{{{project}}}}/sql"
      |
      |spark_schema_repo:
      |  spark_schema_file:
      |    spark_schema_root: ./{config}/schema/spark
      |
      |deltalake_schema_repo:
      |  deltalake_sql_file:
      |    deltalake_schema_root: ./{config}/schema
      |
      |# used to write data lineage to
      |metadata_repo:
      |  metadata_file:
      |    metadata_root: ./config/runs
      |    metadata_dataset: dataset.json
      |    metadata_index: index.json
    """.format(config=config_dir))

    return env_config


def get_local_environment_config(config_dir:str = _CONFIG_DIR):
    env_config = _strip_margin(
    """datalake: "{{{{cwd}}}}/data"
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
      |    pipeline_root: "./{config}/{{{{project}}}}/pipelines"
      |    sql_root: "./{config}/{{{{project}}}}/sql"
      |
      |spark_schema_repo:
      |  spark_schema_file:
      |    spark_schema_root: ./{config}/schema/spark
      |
      |deltalake_schema_repo:
      |  deltalake_sql_file:
      |    deltalake_schema_root: ./{config}/schema/deltalake
      |
      |# used to write data lineage to
      |metadata_repo:
      |  metadata_file:
      |    metadata_root: ./config/runs
      |    metadata_dataset: dataset.json
      |    metadata_index: index.json
    """.format(config=config_dir))

    return env_config


def _get_dot_env_config(config_dir:str = _CONFIG_DIR):
  env_config = _strip_margin(
    """YETL_ROOT=./{config}
      |YETL_ENVIRONMENT=local
    """.format(config=config_dir)) 
  return env_config


def _make_config_file(directory:str, filename:str, data:str, overwrite:bool=False):
    filepath = os.path.join(directory, filename)
    if not os.path.exists(filepath) or overwrite:
        with open(filepath, mode="w", encoding="utf-8") as f:
            f.write(data)

def _make_config_path(project:str, config:dict, repo:SchemaRepo):

  repo_name = repo.name.lower()
  repo_key = repo.value.lower()

  path_config = config[repo_name].get(repo_key)
  if path_config:
    for _, v in path_config.items():
      template: jinja2.Template = jinja2.Template(v)
      dir_path = template.render(project=project)
      dir_path = os.path.abspath(dir_path)
      os.makedirs(dir_path, exist_ok=True)



def init(project:str, home_dir:str=".", config_folder:str=_CONFIG_DIR, overwrite:bool = False):

    
  # make the root dir
  root_dir = os.path.join(home_dir, _CONFIG_DIR)
  root_dir = os.path.abspath(root_dir)
  os.makedirs(root_dir, exist_ok=True)

  # make root configuration files.
  _make_config_file(home_dir, ".env", _get_dot_env_config())
  _make_config_file(root_dir, "logging.yaml", get_log_configuration(), overwrite)

  # make the environment dir and configuration files.
  environment_path = os.path.join(root_dir, "environment")
  os.makedirs(environment_path, exist_ok=True)


  _make_config_file(environment_path, _DBX_CONFIG_FILE, get_dbx_environment_config(config_folder), overwrite)
  _make_config_file(environment_path, _LOCAL_CONFIG_FILE, get_local_environment_config(config_folder), overwrite)

  # load the local configuration file
  local_config_path = os.path.join(environment_path, _LOCAL_CONFIG_FILE)
  with open(local_config_path, "r", encoding="utf-8") as f:
    local_config = yaml.safe_load(f)

  # make the schema repository dirs
  _make_config_path(project, local_config, SchemaRepo.SPARK_SCHEMA_REPO)
  _make_config_path(project, local_config, SchemaRepo.DELTALAKE_SCHEMA_REPO)

  # make project pipeline paths
  _make_config_path(project, local_config, SchemaRepo.PIPELINE_REPO)





    
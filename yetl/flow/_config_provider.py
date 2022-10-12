import yaml
import os
import logging
import json
from enum import Enum


_EXT = "yaml"
_VAR_CWD = "{{cwd}}"


class EnvVariables(Enum):
    YETL_PIPELINE_ROOT = "./config/pipeline"
    YETL_ENVIRONEMNT_ROOT = "./config/environment"
    YETL_ENVIRONMENT = "local"


def _mk_env_path():
    root = os.getenv(
        EnvVariables.YETL_ENVIRONEMNT_ROOT.name,
        EnvVariables.YETL_ENVIRONEMNT_ROOT.value,
    )
    env = os.getenv(
        EnvVariables.YETL_ENVIRONMENT.name, EnvVariables.YETL_ENVIRONMENT.value
    )
    path = f"{root}/{env}.{_EXT}"
    path = os.path.abspath(path)
    return path


def _mk_pipeline_path(pipeline_name: str, project_name: str = "."):
    root = os.getenv(
        EnvVariables.YETL_PIPELINE_ROOT.name,
        EnvVariables.YETL_PIPELINE_ROOT.value,
    )
    path = f"{root}/{project_name}/{pipeline_name}.{_EXT}"
    path = os.path.abspath(path)
    return path


def load_pipeline_config(app_name: str, pipeline_name: str):
    """Loads a spark configuration to load data from landing to raw."""
    _logger = logging.getLogger(app_name)
    path = _mk_pipeline_path(pipeline_name)
    _logger.info(f"Loading Dataflow configuration from file {path}")
    with open(path, "r") as f:
        pipeline = yaml.safe_load(f.read())

    _logger.debug(json.dumps(pipeline, indent=4, sort_keys=True, default=str))

    return pipeline


def load_config(app_name: str):
    _logger = logging.getLogger(app_name)
    path = _mk_env_path()
    _logger.info(f"Loading Dataflow configuration from file {path}")
    with open(path, "r") as f:
        config = f.read()

    config = config.replace(_VAR_CWD, os.getcwd())
    config = yaml.safe_load(config)
    _logger.debug(json.dumps(config, indent=4, sort_keys=True, default=str))

    return config

import yaml
import os
import logging
import json
from enum import Enum
import jinja2

_EXT = "yaml"
_VAR_CWD = "{{cwd}}"


class EnvVariables(Enum):
    YETL_ROOT = "./config"
    YETL_ENVIRONMENT = "local"


def _mk_env_path():
    root = os.getenv(
        EnvVariables.YETL_ROOT.name,
        EnvVariables.YETL_ROOT.value,
    )
    env = os.getenv(
        EnvVariables.YETL_ENVIRONMENT.name, EnvVariables.YETL_ENVIRONMENT.value
    )
    path = f"{root}/environment/{env}.{_EXT}"
    path = os.path.abspath(path)
    return path


def _mk_pipeline_path(pipeline_root: str, pipeline_name: str):
    # TODO - replace with path from environment file.
    path = os.path.join(pipeline_root, f"{pipeline_name}.{_EXT}")
    path = os.path.abspath(path)
    return path


def load_pipeline_config(project:str, root: str, pipeline_name: str):
    """Loads a spark configuration to load data from landing to raw."""
    _logger = logging.getLogger(project)
    path = _mk_pipeline_path(root, pipeline_name)
    _logger.info(f"Loading Dataflow configuration from file {path}")
    with open(path, "r", encoding="utf-8") as f:
        pipeline = yaml.safe_load(f.read())

    _logger.debug(json.dumps(pipeline, indent=4, sort_keys=True, default=str))

    return pipeline


def load_config(project: str):
    _logger = logging.getLogger(project)
    path = _mk_env_path()
    _logger.info(f"Loading Dataflow configuration from file {path}")
    with open(path, "r", encoding="utf-8") as f:
        config = f.read()

    template_partition: jinja2.Template = jinja2.Template(config)

    config = template_partition.render(cwd=os.getcwd(), project=project)
    config = yaml.safe_load(config)
    _logger.debug(json.dumps(config, indent=4, sort_keys=True, default=str))

    return config

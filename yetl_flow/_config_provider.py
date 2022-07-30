import yaml
import os
import logging
import json


_PIPELINE_ROOT = "./config/pipeline"
_EXT = "yaml"
_ENVIRONMENT = "YETL_ENVIRONMENT"
_VAR_CWD = "{{cwd}}"


def _mkpath(name: str):
    """Function that builds the pipeline path"""

    env = os.getenv(_ENVIRONMENT)
    if not env:
        env = "local"

    path = f"{_PIPELINE_ROOT}/{env}/{name}.{_EXT}"
    path = os.path.abspath(path)
    return path


def load_pipeline_config(app_name: str, pipeline_name: str):
    """Loads a spark configuration to load data from landing to raw."""
    _logger = logging.getLogger(app_name)
    path = _mkpath(pipeline_name)
    _logger.info(f"Loading Dataflow configuration from file {path}")
    with open(path, "r") as f:
        pipeline = yaml.safe_load(f.read())

    _logger.debug(json.dumps(pipeline, indent=4, sort_keys=True, default=str))

    return pipeline


def load_config(app_name: str):
    _logger = logging.getLogger(app_name)
    path = _mkpath("config")
    _logger.info(f"Loading Dataflow configuration from file {path}")
    with open(path, "r") as f:
        config = f.read()

    config = config.replace(_VAR_CWD, os.getcwd())
    config = yaml.safe_load(config)
    _logger.debug(json.dumps(config, indent=4, sort_keys=True, default=str))

    return config

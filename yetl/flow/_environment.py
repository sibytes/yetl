import yaml
import os
import logging
import json
import jinja2
from pydantic import BaseSettings, Field, BaseModel, PrivateAttr
from typing import Any

_EXT = "yaml"


class EnvironmentSettings(BaseSettings):

    root: str = Field(default="./config", env="YETL_ROOT")
    environment: str = Field(default="local", env="YETL_ENVIRONMENT")

    class Config:
        env_prefix = "yelt"


class Environment(BaseModel):
    environment_settings: EnvironmentSettings = Field(default=EnvironmentSettings())
    _path: str = PrivateAttr(default=None)
    _logger: Any = PrivateAttr(default=None)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)

    def environment_filepath(self):
        root = self.environment_settings.root
        env = self.environment_settings.environment
        path = os.path.abspath(f"{root}/environment/{env}.{_EXT}")
        return path

    def load(self, project: str):

        path = self.environment_filepath()
        self._logger.debug(f"Loading Dataflow configuration from file {path}")
        with open(path, "r", encoding="utf-8") as f:
            config = f.read()

        template_partition: jinja2.Template = jinja2.Template(config)

        config = template_partition.render(cwd=os.getcwd(), project=project)
        config = yaml.safe_load(config)
        self._logger.debug(json.dumps(config, indent=4, sort_keys=True, default=str))

        return config

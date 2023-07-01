import jinja2
from enum import Enum
import yaml
from pyspark.sql.types import StructType
from typing import Dict
import os
import pkg_resources
import logging


YETL_CONFIG = "YETL_CONFIG"
_ENCODING = "utf-8"
_DBX_WORKSPACE_PATH = "/Workspace"
_DBX_REPO_PATH = "/Workspace/Repos"


class VersionNotFoundException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class JinjaVariables(Enum):
    DATABASE = "database"
    TABLE = "table"
    CHECKPOINT = "checkpoint"
    FILENAME_DATE_FORMAT = "filename_date_format"
    PATH_DATE_FORMAT = "path_date_format"
    CONTAINER = "container"
    DELTA_PROPERTIES = "delta_properties"
    LOCATION = "location"
    PROJECT = "project"
    CATALOG = "catalog"


def is_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def check_version(data: dict):
    _logger = logging.getLogger(__name__)
    version = data.get("version")

    if version is None:
        raise VersionNotFoundException()

    del data["version"]

    pkg_version = pkg_resources.get_distribution("yetl-framework").version
    pkg_version = pkg_version.split(".")

    try:
        version = version.split(".")
        if pkg_version[0] != version[0] or pkg_version[1] != version[1]:
            _logger.warning(
                f"Configuration and library shows that it's incompatible config version config_version={version} package_version={pkg_version}"
            )

    except Exception as e:
        raise VersionNotFoundException from e


def render_jinja(data: str, replacements: Dict[JinjaVariables, str]):
    _logger = logging.getLogger(__name__)
    _logger.debug(f"Rendering Jinja string {data}")
    if data and isinstance(data, str):
        replace = {k.value: v for (k, v) in replacements.items()}
        skip = False
        for k, v in replace.items():
            if v is None and "{{" + k + "}}" in data.replace(" ", ""):
                skip = True
                break

        if not skip:
            template: jinja2.Template = jinja2.Template(data)
            data = template.render(replace)
    _logger.debug(f"Rendered Jinja string {data}")

    return data


def abs_config_path(root: str, path: str):
    _logger = logging.getLogger(__name__)
    if not os.path.isabs(path):
        path = os.path.join(root, path)
    _logger.debug(f"Absolute config path {path}")
    return path


def get_config_path(project: str, path: str):
    _logger = logging.getLogger(__name__)

    default_path = "."
    if is_databricks():
        if os.path.exists(_DBX_WORKSPACE_PATH):
            default_path = f"{_DBX_WORKSPACE_PATH}"

        if os.getcwd().startswith(_DBX_REPO_PATH):
            repo_path = "/".join(os.getcwd().split("/")[0:5])
            default_path = repo_path

    if not path:
        path = os.getenv(YETL_CONFIG, default_path)
    path = os.path.abspath(path)
    path = os.path.join(path, project)
    _logger.info(f"Absolute root config path {path}")
    return path


def load_schema(path: str):
    _logger = logging.getLogger(__name__)
    schema = load_yaml(path)
    _logger.info(f"Loading schema {path}")
    schema = StructType.fromJson(schema)

    return schema


def load_yaml(path: str):
    _logger = logging.getLogger(__name__)
    _logger.info(f"Loading yaml file {path}")
    with open(path, "r", encoding=_ENCODING) as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            location = ""
            if hasattr(e, "problem_mark"):
                mark = e.problem_mark
                location = f"Error position ({mark.line}, {mark.column})"

            if hasattr(e, "problem"):
                problem = f"{e.problem}."

            raise Exception(f"Invalid yaml format in {path}. {problem} {location}")
    _logger.debug(data)
    return data


def load_text(path: str):
    _logger = logging.getLogger(__name__)
    _logger.info(f"Loading text file {path}")
    with open(path, "r", encoding=_ENCODING) as f:
        data = f.read()
    _logger.debug(data)
    return data


def get_ddl(spark_schema: StructType, header: bool = True):
    _logger = logging.getLogger(__name__)
    _logger.debug(f"Converting spark schema to ddl with header={str(header)}")
    if header:
        ddl = [f"{f.name} {f.dataType.simpleString()}" for f in spark_schema.fields]
        _logger.debug(ddl)
    else:
        ddl = [
            f"_c{i} {f.dataType.simpleString()}"
            for i, f in enumerate(spark_schema.fields)
        ]
        _logger.debug(ddl)

    return ddl


def get_html_table(data: dict):
    rows = []
    for k, v in data.items():
        if isinstance(v, dict):
            for ki, vi in v.items():
                row = f"<tr><td>{k}.{ki}</td><td>{vi}</td></tr>"
                rows.append(row)
        else:
            row = f"<tr><td>{k}</td><td>{v}</td></tr>"
            rows.append(row)

    html = "".join(rows)

    html = f"""
  <table>
    <tr><th>Name</th><th>Source</th><th>Destination</th></tr>
    {html}
  </table>
  """

    return html

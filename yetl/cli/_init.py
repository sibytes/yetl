import os
import yaml
import pkg_resources
from ..validation import (
    SchemaFiles,
    get_schema,
)
import json
from importlib.resources import files


def init(project: str, directory: str = "."):
    project = project.lower()
    project_path = os.path.abspath(directory)
    project_path = f"{project_path}/{project}"
    paths: dict = _make_project_dir(project_path, project)
    _create_log_file(project_path)
    _create_json_schema(project_path, paths["pipeline"])
    _create_tables_excel(project_path, paths["pipeline"])

    for _, p in paths.items():
        _make_dirs(project_path, p)


def _make_dirs(project_path: str, relative_path: str):
    relative_path.replace("./", "")
    path = f"{project_path}/{relative_path}"
    os.makedirs(path, exist_ok=True)


def _create_json_schema(project_path: str, pipeline_dir: str):
    """Create json schema files to assist with vscode editing and validation"""

    json_schema_path = os.path.abspath(project_path)
    json_schema_path = os.path.join(json_schema_path, pipeline_dir, "json_schema")
    os.makedirs(json_schema_path, exist_ok=True)

    for f in SchemaFiles:
        schema = get_schema(f)
        schema_path = os.path.join(json_schema_path, f.value)
        with open(schema_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(schema, indent=4))


def _get_default_config(name: str):
    """Get the default configuration"""
    config = files("yetl._resources").joinpath(name).read_text()

    return config


def _get_binary_template(name: str):
    """Get the binary template object"""
    data = files("yetl._resources").joinpath(name).read_bytes()

    return data


def _create_log_file(project_path: str):
    config: dict = yaml.safe_load(_get_default_config("logging.yaml"))
    file_path = os.path.join(project_path, "logging.yaml")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(yaml.safe_dump(config, indent=4))


def _create_tables_excel(project_path: str, pipeline_dir: str):
    data: bytes = _get_binary_template("tables.xlsx")

    pipeline_path = os.path.abspath(project_path)
    pipeline_path = os.path.join(pipeline_path, pipeline_dir)
    file_path = os.path.join(pipeline_path, "tables.xlsx")
    with open(file_path, "wb") as f:
        f.write(data)


def _make_project_dir(project_path: str, project: str):
    config: dict = yaml.safe_load(_get_default_config("project.yaml"))
    config["name"] = project
    config["version"] = pkg_resources.get_distribution("yetl-framework").version

    pipeline_path = config["pipeline"]
    paths = {
        "sql": config["sql"],
        "spark_schema": config["spark_schema"],
        "pipeline": pipeline_path,
        "databricks_notebooks": config["databricks_notebooks"],
        "databricks_workflows": config["databricks_workflows"],
        "databricks_queries": config["databricks_queries"],
    }

    try:
        os.makedirs(project_path, exist_ok=False)
    except Exception as e:
        raise Exception(f"project {project} already exists at this path") from e

    project_file_path = os.path.join(project_path, f"{project}.yaml")
    with open(project_file_path, "w", encoding="utf-8") as f:
        f.write(
            f"# yaml-language-server: $schema={pipeline_path}/json_schema/sibytes_yetl_project_schema.json\n\n"
        )
        f.write(yaml.safe_dump(config, indent=4))

    return paths

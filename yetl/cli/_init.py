import os
import yaml
import pkg_resources


def init(project: str, directory: str = "."):
    project = project.lower()
    project_path = os.path.abspath(directory)
    project_path = f"{project_path}/{project}"
    paths = _make_project_dir(project_path, project)
    _create_log_file(project_path)

    for p in paths:
        _make_dirs(project_path, p)


def _make_dirs(project_path: str, relative_path: str):
    relative_path.replace("./", "")
    path = f"{project_path}/{relative_path}"
    os.makedirs(path, exist_ok=False)


def _create_log_file(project_path: str):
    config = {
        "version": 1,
        "formatters": {
            "default": {
                "format": '"%(levelname)s : %(asctime)s : %(name)s : %(filename)s.%(funcName)s: line(%(lineno)s) : %(message)s"'
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "stream": "ext://sys.stdout",
            }
        },
        "root": {"level": "DEBUG", "handlers": "[console]"},
    }

    file_path = os.path.join(project_path, "logging.yaml")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(yaml.safe_dump(config, indent=4))


def _make_project_dir(project_path: str, project: str):
    sql = "./sql"
    spark_schema = "./schema"
    pipeline = "./pipelines"
    databricks_notebooks = "./databricks/notebooks"
    databricks_workflows = "./databricks/workflows"
    databricks_queries = "./databricks/queries"
    paths = [
        sql,
        spark_schema,
        pipeline,
        databricks_notebooks,
        databricks_workflows,
        databricks_queries,
    ]

    spark = {
        "logging_level": "ERROR",
        "config": {
            "spark.master": "local",
            "spark.databricks.delta.allowArbitraryProperties.enabled": True,
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        },
    }

    config = {
        "version": pkg_resources.get_distribution("yetl-framework").version,
        "name": project,
        "sql": sql,
        "spark_schema": spark_schema,
        "pipeline": pipeline,
        "databricks_notebooks": databricks_notebooks,
        "databricks_workflows": databricks_workflows,
        "databricks_queries": databricks_queries,
        "spark": spark,
    }

    try:
        os.makedirs(project_path, exist_ok=False)
    except Exception as e:
        raise Exception(f"project {project} already exists at this path") from e

    project_file_path = os.path.join(project_path, f"{project}.yaml")
    with open(project_file_path, "w", encoding="utf-8") as f:
        f.write(yaml.safe_dump(config, indent=4))

    return paths

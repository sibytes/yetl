from .cli.manifest_tables import FileSource
import typer
from .cli.metadata import MetadataFileStore
from .cli.build import build_config
from .cli.init import init as _init
from .cli.project import Project
import os
import yaml
import json

app = typer.Typer()


@app.command()
def init(
    project: str,
    home_dir: str = ".",
    config_folder: str = "config",
    overwrite: bool = False,
):
    """Initialise the configuration directory with the required structure and start config files

    --home-dir:str - What you want the configuration folder to be built

    --config-folder:str - The name of the configuration folder

    --overwrite:bool - Overwrite the files already there with defaults, any new files added will remain.
    """
    _init(project, home_dir, config_folder, overwrite)


@app.command()
def build(
    project: str,
    metadata_file: str,
    template_file: str,
    build_dir: str,
):
    """Use table manifest file and the pipeline jinja template to build a pipeline configuration for each table"""
    # build_config(project, metadata_file, template_file, build_dir)

    project_dir = os.path.abspath(os.path.join(build_dir, "project", project))

    metadata_path = os.path.join(project_dir, metadata_file)
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata: dict = yaml.safe_load(f)

    project = Project(**metadata)
    print(json.dumps(json.loads(project.json()), indent=4))

@app.command()
def create_table_manifest(
    project: str,
    build_dir: str,
    source_type: str,
    source_dir: str,
    filename: str = "*",
    extract_regex: str = None,
):
    """Create manifest configuration file containing the names of tables we want create yetl data pipelines on

    --filename:str - A wildcard name to filter the files you want to include to derive table names

    --extract_regex:str - A regex expression used to extract the table name from the file to get the table names you want e.g. remove timestamps ect
    """
    source = FileSource(source_dir, filename, extract_regex)
    files = source.tables()
    metadata = MetadataFileStore(project, build_dir)
    metadata.save_table_manifest(files)


if __name__ in ["yetl.__main__", "__main__"]:
    app()

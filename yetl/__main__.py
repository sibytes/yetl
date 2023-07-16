import typer
from .cli import _init
from typing_extensions import Annotated
from .cli.metadata_provider import XlsMetadata, ImportFormat
from .config import Config
from typing import Optional
import logging

app = typer.Typer()


@app.command()
def init(project: str, directory: str = "."):
    """Initialise the project directory with the suggested structure and start config files

    --directory:str - Where you want the project to be initialised
    """
    _init.init(project, directory)


@app.command()
def import_tables(
    source: str,
    destination: str,
    format: Annotated[
        ImportFormat, typer.Option(case_sensitive=False)
    ] = ImportFormat.excel,
):
    """Import tables configuration from an external source such as a Excel.

    source:str - The uri indicator of the table metadata e.g. the file path if importing a csv \n
    format:ImportFormat -  The format of the table metadata to import e.g. excel
    """
    metadata = XlsMetadata(source=source)
    metadata.write(path=destination)


@app.command()
def validate(
    project: Annotated[str, typer.Argument()],
    pipeline: Annotated[str, typer.Argument()],
    config_path: Annotated[Optional[str], typer.Argument()] = None,
):
    """Validate that configuration meets the schema and deserialises.

    projec:str - Name of the project to validate \n
    pipeline:str - Name of the pipeline config to validate \n
    config_path:str - Path to the project configuration root \n

    """
    _logger = logging.getLogger(__name__)

    _logger.info(f"validating project {project} {pipeline}")
    Config(project=project, pipeline=pipeline, config_path=config_path)
    _logger.info(f"{project} {pipeline} is Valid!")


if __name__ in ["yetl.__main__", "__main__"]:
    app()

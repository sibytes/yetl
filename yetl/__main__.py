import typer
from .cli import _init
from typing_extensions import Annotated
from .cli.metadata_provider import XlsMetadata, ImportFormat

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

    --source:str - The uri indicator of the table metadata e.g. the file path if importing a csv
    --format:ImportFormat -  The format of the table metadata to import e.g. excel
    """
    metadata = XlsMetadata(source=source)
    metadata.write(path=destination)


@app.command()
def build():
    """Build assets such as databricks workflows."""
    # TODO: implement workflow build
    pass


if __name__ in ["yetl.__main__", "__main__"]:
    app()

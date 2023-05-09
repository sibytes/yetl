import typer
from .cli import _init

app = typer.Typer()


@app.command()
def init(project: str, directory: str = "."):
    """Initialise the project directory with the suggested structure and start config files

    --directory:str - Where you want the project to be initialised
    """
    _init.init(project, directory)


@app.command()
def import_tables(location: str, format: str = "csv"):
    """Import tables configuration from an external source such as a CSV.

    --location:str - The uri indicator of the table metadata e.g. the file path if importing a csv
    --format:str -  The format of the table metadata to import e.g. CSV
    """
    # TODO: implement CSV import of tables config
    pass


@app.command()
def build():
    """Build assets such as databricks workflows."""
    # TODO: implement workflow build
    pass


if __name__ in ["yetl.__main__", "__main__"]:
    app()

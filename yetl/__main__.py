import typer
from .cli import _init

app = typer.Typer()


@app.command()
def init(project: str, directory: str = "."):
    """Initialise the project directory with the suggested structure and start config files

    --home-dir:str - Where you want the project to be initialised
    """
    _init.init(project, directory)


if __name__ in ["yetl.__main__", "__main__"]:
    app()

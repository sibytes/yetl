from .cli.manifest_tables import FileSource
import typer
from .cli.metadata import MetadataFileStore
from .cli.build import build_config

app = typer.Typer()


@app.command()
def build(
    project: str,
    metadata_file: str,
    template_file: str,
    build_dir: str,
):
    build_config(project, metadata_file, template_file, build_dir)


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

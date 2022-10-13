from .cli.manifest_tables import FileSource
import typer

app = typer.Typer()

@app.command()
def manifest_tables(source_type: str, directory: str, filename_mask: str, extract_regex: str):
    source = FileSource(directory, filename_mask, extract_regex)
    files = source.tables()
    print(files)



if __name__ in ["yetl.__main__", "__main__"]:
    app()





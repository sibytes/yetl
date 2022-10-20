from pipes import Template
from .cli.manifest_tables import FileSource
import typer
from .cli.metadata import MetadataFileStore
from jinja2 import (
    Environment,
    FileSystemLoader,
    Undefined,
    DebugUndefined,
    Template,
    ChainableUndefined,
)
from jinja2.utils import missing, object_type_repr
import os
import yaml
import shutil

app = typer.Typer()


class YetlDebugUndefined(Undefined):

    __slots__ = ()

    def __str__(self) -> str:
        if self._undefined_hint:
            message = f"undefined value printed: {self._undefined_hint}"

        elif self._undefined_obj is missing:
            message = self._undefined_name  # type: ignore

        else:
            message = (
                f"no such element: {object_type_repr(self._undefined_obj)}"
                f"[{self._undefined_name!r}]"
            )

        return f"{{{{ {message} }}}}"

    def _fail_with_undefined_error(self, *args, **kwargs):
        return ""


@app.command()
def build(
    project: str,
    project_dir: str,
    metadata_file: str,
    template_file: str,
    build_dir: str,
):

    # landing/{{timeslice_path_date_format}}/{{adworks_tables_table_name}}_{{timeslice_file_date_format}}.jsonl
    project_dir = os.path.abspath(project_dir)
    template_dir = os.path.join(project_dir, project, "pipeline")

    metadata_path = os.path.join(project_dir, project, metadata_file)

    build_path = os.path.abspath(build_dir)
    if os.path.exists(build_path):
        shutil.rmtree(build_path)

    pipeline_build_path = os.path.join(build_path, "pipeline")
    os.makedirs(pipeline_build_path)

    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata: dict = yaml.safe_load(f)

    # environment = Environment(loader=FileSystemLoader(template_dir), undefined=Undefined)
    # template = environment.get_template(template_file)
    with open(os.path.join(template_dir, template_file), "r", encoding="utf-8") as f:
        template_data = f.read()

    template: Template = Template(template_data, undefined=YetlDebugUndefined)

    tables = metadata.get("tables", [])
    for table in tables:
        table_name = table["table"]
        filename = f"{table_name}_{template_file}"
        filename = os.path.join(pipeline_build_path, filename)
        data = {
            f"{project}_tables_table_name": table_name,
            "timeslice_path_date_format": "timeslice_path_date_format",
            "timeslice(file_date_format)": "timeslice(file_date_format)",
        }
        content = template.render(data)
        with open(filename, mode="w", encoding="utf-8") as pipeline:
            pipeline.write(content)


@app.command()
def create_table_manifest(
    project: str,
    project_dir: str,
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
    metadata = MetadataFileStore(project, project_dir)
    metadata.save_table_manifest(files)


if __name__ in ["yetl.__main__", "__main__"]:
    app()

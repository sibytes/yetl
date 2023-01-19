import os
from jinja2 import Template, Undefined
import shutil
import yaml
from jinja2.utils import missing, object_type_repr
from .project import Project


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


def _to_yaml(data: dict):
    if isinstance(data, dict):
        yaml_string = yaml.dump(data, default_flow_style=False)
    else:
        raise Exception("to_yaml function expects a dictionary type.")
    return yaml_string


def build_config(
    project: str,
    metadata_file: str,
    template_file: str,
    build_dir: str,
):

    project_dir = os.path.abspath(os.path.join(build_dir, "project", project))
    template_dir = project_dir

    metadata_path = os.path.join(project_dir, metadata_file)

    build_path = os.path.abspath(build_dir)
    pipeline_build_path = os.path.join(build_path, project, "pipelines")
    if os.path.exists(pipeline_build_path):
        shutil.rmtree(pipeline_build_path)
    os.makedirs(pipeline_build_path)

    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata: dict = yaml.safe_load(f)

    with open(os.path.join(template_dir, template_file), "r", encoding="utf-8") as f:
        template_data = f.read()

    project: Project = Project(**metadata)
    template: Template = Template(
        template_data,
        undefined=YetlDebugUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env = template.environment
    env.globals.update(to_yaml=_to_yaml)

    for table in project.tables:
        filename = f"{table.name}_{template_file}"
        filename = os.path.join(pipeline_build_path, filename)
        data = table.dict()
        content = template.render(table=data)
        with open(filename, mode="w", encoding="utf-8") as pipeline:
            pipeline.write(content)

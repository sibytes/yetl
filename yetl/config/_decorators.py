# implicit, not referenced - must be the 1st import
from ._logging_config import configure_logging
import logging
from ._config import Config
from ._timeslice import Timeslice
from ._stage_type import StageType
from .table import Table


def yetl_flow(
    stage: StageType,
    project: str,
    pipeline: str = None,
    config_path: str = None,
    catalog: str = None,
    catalog_enabled: bool = True,
):
    def decorate(function):
        def wrap_function(*args, **kwargs):
            configure_logging(project)
            _logger = logging.getLogger(__name__)

            _pipeline = pipeline
            if not _pipeline:
                _pipeline = function.__name__

            _logger.info(f"Loading pipeline configuration {_pipeline}")

            timeslice = kwargs.get("timeslice", Timeslice(day="*", month="*", year="*"))
            if "timeslice" in kwargs.keys():
                del kwargs["timeslice"]

            try:
                table = kwargs["table"]
                del kwargs["table"]
            except KeyError as e:
                raise Exception(f"{e} is a required argument for a yetl flow function")

            config = Config(
                project=project,
                pipeline=_pipeline,
                config_path=config_path,
                timeslice=timeslice,
            )
            table_mapping = config.get_table_mapping(
                stage=stage,
                table=table,
                catalog=catalog,
                catalog_enabled=catalog_enabled,
            )

            destination: Table = table_mapping.destination
            sources = table_mapping.source
            if isinstance(sources, dict):
                for _, source in sources.items():
                    config.set_checkpoint(source=source, destination=destination)
            else:
                config.set_checkpoint(source=sources, destination=destination)

            _logger.info(f"Calling function {function.__name__}")
            ret = function(
                *args,
                table_mapping=table_mapping,
                **kwargs,
            )
            return ret

        return wrap_function

    return decorate

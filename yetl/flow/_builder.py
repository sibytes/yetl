from .context import IContext, context_factory
from .audit import Audit
from datetime import datetime
from ._environment import Environment
from .dataflow import IDataflow, Dataflow
from .dataset import dataset_factory
import json
import logging

_logger = logging.getLogger(__name__)


def _build_context(pipeline_name: str, project: str, function_name: str, kwargs: dict):

    # load the environment settings and configuration provider
    environment = Environment()

    # default the name to the function name of the deltaflow
    if not pipeline_name:
        name = function_name

    else:
        table = kwargs.get("table")
        name = f"{table}_{pipeline_name}"

    audit_kwargs = {k: str(v) for k, v in kwargs.items()}
    auditor = Audit()
    auditor.dataflow({"name": name})
    auditor.dataflow({"args": audit_kwargs})
    auditor.dataflow({"started": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    auditor.dataflow({"started_utc": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

    timeslice = kwargs.get("timeslice")
    if "timeslice" in kwargs.keys():
        del kwargs["timeslice"]

    config = environment.load(project=project)
    # create the context for the pipeline to run
    context: IContext = context_factory.get_context_type(
        project=project,
        name=name,
        auditor=auditor,
        timeslice=timeslice,
        environment=environment,
        config=config,
    )

    # run the pipeline
    _logger.debug(f"Executing Dataflow {context.project} with timeslice={timeslice}")

    return context


def _build_dataflow(context: IContext):

    dataflow: IDataflow = Dataflow(context=context)

    dataflow_config = context.pipeline_repository.load_pipeline(context.name)
    dataflow_config = dataflow_config.get("dataflow")

    for database, table in dataflow_config.items():
        for table, table_config in table.items():
            md = dataset_factory.get_dataset_type(
                context, database, table, dataflow.dataflow_id, table_config
            )
            _logger.debug(
                f"Deserialized {database}.{table} configuration into {type(md)}"
            )
            dataflow.append(md)

            dataflow.audit_lineage()

    return dataflow

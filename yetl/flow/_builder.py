from .context import IContext, context_factory
from .audit import Audit
from datetime import datetime
from ._environment import Environment


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
    context.log.info(f"Executing Dataflow {context.project} with timeslice={timeslice}")

    return context

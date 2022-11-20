# implicit, not referenced - must be the 1st import
from . import _logging_config

from .context import SparkContext
from .audit import Audit
from datetime import datetime


class YetlFlowException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def yetl_flow(project: str, pipeline_name: str = None):
    def decorate(function):
        def wrap_function(*args, **kwargs):

            # default the name to the function name of the deltaflow

            if not pipeline_name:
                _name = function.__name__

            else:
                table = kwargs.get("table")
                _name = f"{table}_{pipeline_name}"

            audit_kwargs = {k: str(v) for k, v in kwargs.items()}
            auditor = Audit()
            auditor.dataflow({"name": _name})
            auditor.dataflow({"args": audit_kwargs})
            auditor.dataflow({"started": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            auditor.dataflow(
                {"started_utc": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            )

            timeslice = kwargs.get("timeslice")
            if "timeslice" in kwargs.keys():
                del kwargs["timeslice"]

            # TODO: abstract out spark context to IContext
            # create the context for the pipeline to run
            context = SparkContext(project, _name, auditor, timeslice)

            # run the pipeline
            context.log.info(
                f"Executing Dataflow {context.project} with timeslice={timeslice}"
            )

            try:
                function(
                    context=context,
                    dataflow=context.dataflow,
                    timeslice=timeslice,
                    *args,
                    **kwargs,
                )
            except Exception as e:
                msg = f"Dataflow application {context.project} failed due to {e}."
                context.log.error(msg)
                auditor.error(e)

            # get the delta lake audit information and add it to the return
            auditor.dataflow({"finished": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            auditor.dataflow(
                {"finished_utc": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            )
            return auditor.audit_log

        return wrap_function

    return decorate

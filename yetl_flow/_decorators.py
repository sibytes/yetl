from . import _logging_config  # must be the 1st import
from ._context import Context
from .audit import Audit
from datetime import datetime


class YetlFlowException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def yetl_flow(name: str = None, app_name: str = None, log_level="INFO"):
    def decorate(function):
        def wrap_function(*args, **kwargs):

            # default the name to the function name of the deltaflow
            if not name:
                _name = function.__name__

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

            # create the context for the pipeline to run
            context = Context(app_name, log_level, _name, auditor, timeslice)

            # run the pipeline
            context.log.info(
                f"Executing Dataflow {context.app_name} with timeslice={timeslice}"
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
                msg = f"Dataflow application {context.app_name} failed due to {e}."
                context.log.error(msg)
                raise e

            # get the delta lake audit information and add it to the return
            auditor.dataflow({"finished": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            auditor.dataflow(
                {"finished_utc": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            )
            return auditor.audit_log

        return wrap_function

    return decorate

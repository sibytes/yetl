# implicit, not referenced - must be the 1st import
from . import _logging_config
from ._builder import _build_context
from datetime import datetime


class YetlFlowException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def yetl_flow(project: str, pipeline_name: str = None):
    def decorate(function):
        def wrap_function(*args, **kwargs):

            function_name = function.__name__
            context = _build_context(
                pipeline_name=pipeline_name,
                project=project,
                function_name=function_name,
                kwargs=kwargs,
            )
            timeslice = context.timeslice
            auditor = context.auditor

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

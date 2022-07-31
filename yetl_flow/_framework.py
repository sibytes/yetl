from . import _logging_config  # must be the 1st import
from ._context import Context
from ._delta_lake import get_audits
import time


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

            spark = kwargs.get("spark")
            timeslice = kwargs.get("timeslice")

            # create the context for the pipeline to run
            context = Context(app_name, log_level, _name, spark, timeslice)

            # run the pipeline
            yetl_flow_exception = None
            while context.dataflow.retries >= 0:
                try:
                    function(
                        context=context, dataflow=context.dataflow, *args, **kwargs
                    )
                    break
                except Exception as e:
                    msg = f"Dataflow application {context.app_name} failed due to {e}."
                    context.dataflow.retries -= 1
                    context.log.error(msg)
                    yetl_flow_exception = e
                    if context.dataflow.retries >= 0:
                        context.log.info(
                            f"Retrying {context.app_name} in after {context.dataflow.retry_wait} seconds; {context.dataflow.retries} retries are remaning."
                        )
                        time.sleep(context.dataflow.retry_wait)

            if yetl_flow_exception:
                msg = f"Dataflow application {context.app_name} failed due to {yetl_flow_exception}. No retries remaining."
                raise YetlFlowException(msg) from yetl_flow_exception

            # get the delta lake audit information and add it to the return
            audit = get_audits(context)
            audit = {"deltalake_log": audit}

            return audit

        return wrap_function

    return decorate

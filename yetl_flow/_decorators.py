from . import _logging_config  # must be the 1st import
from ._context import Context
from ._delta_lake import get_audits
import time
from typing import Type


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
            if "timeslice" in kwargs.keys():
                del kwargs["timeslice"]

            # create the context for the pipeline to run
            context = Context(app_name, log_level, _name, spark, timeslice)

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
            audit = {}

            return audit

        return wrap_function

    return decorate

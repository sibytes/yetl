from ._databricks_context import DatabricksContext
from ._i_context import IContext
from ._spark_context import SparkContext
from ._context_options import ContextType
import logging


class _ContextFactory:
    def __init__(self) -> None:
        self._context = {}
        self._logger = logging.getLogger(self.__class__.__name__)

    def register_context_type(self, c_type: ContextType, context_type: type):
        self._logger.debug(f"Register context type {context_type} as {type}")
        self._context[c_type] = context_type

    def get_context_type(
        self, project, name, auditor, timeslice, environment, config: dict
    ) -> IContext:

        engine = config.get("engine")
        context_type_config: str = next(iter(engine))
        context_type: ContextType = ContextType(context_type_config)

        self._logger.debug(f"Setting Context using type {context_type}")
        context: IContext = self._context.get(context_type)

        if not context:
            self._logger.error(
                f"ContextType {context_type.name} not registered in the context factory"
            )
            raise ValueError(context_type)

        config["context_type"] = context_type

        return context(
            project=project,
            name=name,
            auditor=auditor,
            timeslice=timeslice,
            environment=environment,
            **config,
        )


factory = _ContextFactory()
factory.register_context_type(ContextType.SPARK, SparkContext)
factory.register_context_type(ContextType.DATABRICKS, DatabricksContext)

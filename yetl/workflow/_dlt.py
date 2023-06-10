from ..config import StageType, Config
from ..config.table import Table
from typing import Callable
import logging

_logger = logging.getLogger(__name__)


def create_dlt(
    config: Config,
    stage: StageType,
    dlt_funct: Callable[[Table, Table], None],
    debug: bool = False,
    **kwargs,
):
    tables = config.tables.lookup_table(
        stage=stage,
        first_match=False,
        # this will filter the tables on a custom property
        # in the tables parameter you can add whatever custom properties you want
        # either for filtering or to use in pipelines
        **kwargs,
    )

    for t in tables:
        table_mapping = config.get_table_mapping(
            stage=stage,
            table=t.table,
            # dlt does this so yetl doesn't need to
            create_database=False,
            create_table=False,
        )
        # TODO: not sure if we need checkpoints in DLT
        # config.set_checkpoint(
        #     table_mapping.source, table_mapping.destination
        # )
        src = table_mapping.source
        dst = table_mapping.destination
        if debug:
            msg = f"{src.database}.{src.table} => {dst.database}.{dst.table}"
            _logger.info(msg)
        else:
            dlt_funct(table_mapping.source, table_mapping.destination)

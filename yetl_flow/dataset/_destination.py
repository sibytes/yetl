from ._dataset import Dataset
from pyspark.sql import DataFrame
from pyspark.sql import functions as fn
from ..parser._constants import *
import json
from ..audit import Audit


class Destination(Dataset):
    def __init__(
        self,
        context,
        database: str,
        table: str,
        dataset: dict,
        io_type: str,
        auditor: Audit,
    ) -> None:
        super().__init__(context, database, table, dataset, io_type, auditor)

        self.auto_io: bool
        self.dataframe: DataFrame

    def write(self):

        self.context.log.debug(
            f"Reordering sys_columns to end for {self.database_table} from {self.path} {CONTEXT_ID}={str(self.context_id)}"
        )
        # remove a re-add the _context_id since there will be dupplicate columns
        # when dataframe is built from multiple sources.
        self.dataframe = self.dataframe.drop(CONTEXT_ID).withColumn(
            CONTEXT_ID, fn.lit(str(self.context_id))
        )

        # clean up the column ordering
        sys_columns = [c for c in self.dataframe.columns if c.startswith("_")]
        data_columns = [c for c in self.dataframe.columns if not c.startswith("_")]
        data_columns = data_columns + sys_columns
        self.dataframe = self.dataframe.select(*data_columns)

        # get the partitions values for efficient IO patterns
        self.partition_values = self._get_partitions_values()

        if self.partition_values:
            msg_partition_values = json.dumps(self.partition_values, indent=4)
            self.context.log.info(
                f"""IO operations for {self.database}.{self.table} will be paritioned by: \n{msg_partition_values}"""
            )

        self.save.write()
        self.save_metadata()

    def _get_partitions_values(self):

        partition_values = {}
        if self.partitions:
            partition_values_df = self.dataframe.select(*self.partitions).distinct()

            for p in self.partitions:
                group_by: list = list(self.partitions)
                group_by.remove(p)
                if group_by:
                    partition_values_df = partition_values_df.groupBy(*group_by).agg(
                        fn.collect_set(p).alias(p)
                    )
                else:
                    partition_values_df = partition_values_df.withColumn(
                        p, fn.collect_set(p)
                    )

            partition_values_df = partition_values_df.collect()
            partition_values = partition_values_df[0].asDict()

        return partition_values

    def is_source(self):
        return False

    def is_destination(self):
        return True

    def save_metadata(self):
        return super().save_metadata()

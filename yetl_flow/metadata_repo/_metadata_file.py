from ._imetadata_repo import IMetadataRepo
from uuid import UUID
from ..dataset import Dataset
from datetime import datetime


class MetadataFile(IMetadataRepo):

    _DEFAULT_DATASET_FILENAME = "dataset.jsonl"
    _DEFAULT_INDEX_FILENAME = "index.jsonl"
    _DEFUALT_ROOT = "./config/runs"

    _ROOT = "metadata_root"
    _DATASET = "metadata_dataset"
    _INDEX = "metadata_index"

    def __init__(self, context, config: dict) -> None:
        super().__init__(context, config)
        _config = config["metadata_file"]
        self.root = _config.get(self._ROOT, self._DEFUALT_ROOT)
        self.dataset_filename = _config.get(
            self._DATASET, self._DEFAULT_DATASET_FILENAME
        )
        self.index_filename = _config.get(self._INDEX, self._DEFAULT_INDEX_FILENAME)

    def _get_metadata(self,  dataset: Dataset):
        metadata = {
           "id" : dataset.id,
           "database": dataset.database,
           "table": dataset.table,
           "load_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
           "load_timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
           "filename": dataset.path,
           "timeslice": ""

        }

    def save(self, dataset: Dataset):
        metadata = self._get_metadata(dataset)

            # .withColumn("_yetl_lineage",
            #     fn.struct(
            #         fn.struct(
            #             fn.lit(str(self.id)).alias("id"),
            #             fn.lit(self.table).alias("database"),
            #             fn.lit(self.table).alias("table"),
            #             fn.current_timestamp().alias(LOAD_TIMESTAMP),
            #             fn.input_file_name().alias(FILENAME),
            #             # TODO: needs to be fixed to put in the data timeslice not the load timeslice
            #             # fn.lit(self.timeslice).alias(TIMESLICE)
            #         ).alias(f"{self.database}.{self.table}")
            #     )
            # )
            # .withColumn("_yetl_lineage_index",
            #     fn.struct(
            #         fn.struct(
            #             fn.lit(f"{self.database}.{self.table}").alias(str(self.id)),
            #             fn.array(fn.array()).alias("depends_on_id")
            #         )
            #     )
            # )

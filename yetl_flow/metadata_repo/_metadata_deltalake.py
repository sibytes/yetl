from ._imetadata_repo import IMetadataRepo
from uuid import UUID
from ..dataset import Dataset


class MetadataDeltalake(IMetadataRepo):

    _DEFAULT_DATASET_TABLE = "dataset"
    _DEFAULT_INDEX_TABLE = "index"
    _DEFUALT_DATABSE = "yetl"

    _DATABASE = "metadata_database"
    _DATASET = "metadata_table"
    _INDEX = "metadata_index"

    def __init__(self, context, config: dict) -> None:

        super().__init__(context, config)
        _config = config["metadata_deltalake"]

        self.database = _config.get(self._ROOT, self._DEFUALT_DATABSE)
        self.dataset_table = _config.get(self._DATASET, self._DEFAULT_DATASET_TABLE)
        self.index_table = _config.get(self._INDEX, self._DEFAULT_INDEX_TABLE)

    def save(self, dataset: Dataset):
        return super().save(dataset)

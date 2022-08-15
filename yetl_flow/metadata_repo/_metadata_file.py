from ._imetadata_repo import IMetadataRepo
from uuid import UUID


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

    def save(self, correlation_id: UUID, metadata: dict):
        return super().save(correlation_id, metadata)

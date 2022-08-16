import imp
from ._imetadata_repo import IMetadataRepo
from uuid import UUID
from ..dataset import Dataset
from datetime import datetime
from ..file_system import FileFormat


class MetadataFile(IMetadataRepo):

    _DEFAULT_DATASET_FILENAME = "dataset"
    _DEFAULT_INDEX_FILENAME = "index"
    _DEFAULT_EXT = FileFormat.JSON
    _DEFUALT_ROOT = "./config/runs"

    _ROOT = "metadata_root"
    _DATASET = "metadata_dataset"
    _INDEX = "metadata_index"

    def __init__(self, context, config: dict) -> None:
        super().__init__(context, config)
        _config = config["metadata_file"]
        self.root = _config.get(self._ROOT, self._DEFUALT_ROOT)
        self.dataset_filename = _config.get(
            self._DATASET,
            f"{self._DEFAULT_DATASET_FILENAME}.{self._DEFAULT_EXT.name.lower()}",
        )
        self.index_filename = _config.get(
            self._INDEX,
            f"{self._DEFAULT_INDEX_FILENAME}.{self._DEFAULT_EXT.name.lower()}",
        )

    def _get_dataset(self, dataset: Dataset):
        metadata = {
            str(dataset.id): {
                "correlation_id": dataset.correlation_id,
                "database": dataset.database,
                "table": dataset.table,
                "load_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "load_timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "filename": dataset.path,
                "timeslice": "",
            }
        }

        return metadata

    def _get_index(self, dataset: Dataset):
        index = {str(dataset.id): {"depends_on": []}}
        return index

    def save(self, dataset: Dataset):
        metadata = self._get_dataset(dataset)
        index = self._get_index(dataset)

        dir_path = f"{self.root}/{dataset.correlation_id}"
        self.context.fs.mkdirs(dir_path)

        filepath = f"{dir_path}/{self.dataset_filename}"
        self.context.fs.append_file(filepath, metadata, FileFormat.JSON)
        filepath = f"{dir_path}/{self.index_filename}"
        self.context.fs.append_file(filepath, index, FileFormat.JSON)

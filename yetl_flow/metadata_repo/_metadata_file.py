from ._imetadata_repo import IMetadataRepo
from uuid import UUID
from ..dataset import Dataset
from datetime import datetime
from ..file_system import FileFormat
from collections import ChainMap
import json


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

    def _merge_existing(
        self, metadata: dict, context_id: UUID, dir_path: str, file_path: str
    ):
        """If a file doesn't exist write a new data file. If a file does exist the read the contents
        merge with new data and write the file. Because at the moment it's a file per dataflow
        we just re-write the file since it won't be that big.

        """
        str_context_id = str(context_id)
        if not self.context.fs.exists(dir_path):
            self.context.fs.mkdirs(dir_path)

        if not self.context.fs.exists(file_path):
            metadata = {str_context_id: metadata}

        else:
            existing_metadata: dict = self.context.fs.read_file(
                file_path, FileFormat.JSON
            )

            metadata = ChainMap(existing_metadata[str_context_id], metadata)
            metadata = {str_context_id: dict(metadata)}

        return metadata

    def save(self, dataset: Dataset):

        dir_path = f"{self.root}/{dataset.context_id}"
        dataset_file_path = f"{dir_path}/{self.dataset_filename}"
        index_file_path = f"{dir_path}/{self.index_filename}"

        if not self.context.fs.exists(dir_path):
            self.context.fs.mkdirs(dir_path)

        metadata_dataset: dict = self._get_dataset(dataset)
        metadata_index: dict = self._get_index(dataset)

        metadata_dataset = self._merge_existing(
            metadata_dataset, dataset.context_id, dir_path, dataset_file_path
        )
        self.context.log.debug(json.dumps(metadata_dataset, indent=4, default=str))
        self.context.fs.write_file(dataset_file_path, metadata_dataset, FileFormat.JSON)

        metadata_index = self._merge_existing(
            metadata_index, dataset.context_id, dir_path, index_file_path
        )
        self.context.log.debug(json.dumps(metadata_index, indent=4, default=str))
        self.context.fs.write_file(index_file_path, metadata_index, FileFormat.JSON)

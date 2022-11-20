from ..dataset import Dataset
from datetime import datetime


class IMetadataRepo:
    def __init__(self, context, config: dict) -> None:
        self.context = context

    def _get_dataset(self, dataset: Dataset):
        metadata = {
            str(dataset.id): {
                "context_id": str(dataset.context_id),
                "database": dataset.database,
                "table": dataset.table,
                "load_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "load_timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "path": dataset.path,
                "timeslice": "",
            }
        }

        return metadata

    def _get_index(self, dataset: Dataset):

        depends_on_ids = []
        # add the source dataset id's into the attribute list
        if dataset.is_destination():
            sources: dict = self.context.dataflow.sources
            depends_on_ids = [str(v.id) for k, v in sources.items()]

        index = {str(dataset.id): {"depends_on": depends_on_ids}}
        return index

    def save(self, dataset: Dataset):
        """Save a schema into the repo."""
        pass

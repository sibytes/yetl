
from ..dataset import Dataset
from datetime import datetime

class IMetadataRepo:
    def __init__(self, context, config: dict) -> None:
        self.context = context

    def _get_dataset(self, dataset: Dataset):
        metadata = {
            str(dataset.id): {
                "correlation_id": str(dataset.correlation_id),
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
        """Save a schema into the repo."""
        pass
    

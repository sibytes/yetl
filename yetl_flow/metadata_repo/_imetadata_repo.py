from uuid import UUID
from ..dataset import Dataset


class IMetadataRepo:
    def __init__(self, context, config: dict) -> None:
        self.context = context

    def save(self, dataset: Dataset):
        """Save a schema into the repo."""
        pass

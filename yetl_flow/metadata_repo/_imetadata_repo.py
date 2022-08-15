from uuid import UUID


class IMetadataRepo:
    def __init__(self, context, config: dict) -> None:
        self.context = context

    def save(self, correlation_id: UUID, metadata: dict):
        """Save a schema into the repo."""
        pass

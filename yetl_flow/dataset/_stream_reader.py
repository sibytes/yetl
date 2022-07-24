from ._source import Source


class StreamReader(Source):
    def __init__(
        self, context, database: str, table: str, dataset: dict, io_type: str
    ) -> None:
        super().__init__(context, database, table, dataset, io_type)

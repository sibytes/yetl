from ._dataset import Dataset
from ..audit import Audit


class StreamReader(Dataset):
    def __init__(
        self,
        context,
        database: str,
        table: str,
        dataset: dict,
        io_type: str,
        auditor: Audit,
    ) -> None:
        super().__init__(context, database, table, dataset, io_type, auditor)

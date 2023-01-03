import os
from typing import List
import yaml


class MetadataStore:
    def save_tables(self, tables: List[str]):
        pass


class MetadataFileStore(MetadataStore):
    def __init__(self, project: str, directory_path: str) -> None:

        super().__init__()

        self.directory_path = os.path.abspath(directory_path)
        self.directory_path = os.path.join(self.directory_path, project)
        self.project = project

        os.makedirs(self.directory_path, exist_ok=True)

    def _format_table(
        self,
        name: str,
        enable_exceptions: bool = False,
        disable_thresholds: bool = True,
    ):
        table = {"name": name, "keys": [], "enable_exceptions": enable_exceptions}
        if enable_exceptions and not disable_thresholds:
            table["thresholds"] = {
                "thresholds": {
                    "warning": {
                        "min_rows": 1,
                        "max_rows": 100000000,
                        "exception_count": 0,
                        "exception_percent": 0,
                    },
                    "error": {
                        "min_rows": 0,
                        "max_rows": 100000000,
                        "exception_count": 0,
                        "exception_percent": 80,
                    },
                }
            }
        return dict(table)

    def save_tables(
        self,
        tables: List[str],
        enable_exceptions: bool = False,
        disable_thresholds: bool = True,
    ):

        path = os.path.join(self.directory_path, f"{self.project}_tables.yml")
        tables = [
            self._format_table(t, enable_exceptions, disable_thresholds) for t in tables
        ]
        data = {"database": self.project, "tables": tables}
        data_yml = yaml.safe_dump(data)
        with open(path, mode="w", encoding="utf-8") as f:
            f.write(data_yml)

import os
from typing import List
import yaml


class MetadataStore:
    def save_table_manifest(self, tables: List[str]):
        pass


class MetadataFileStore(MetadataStore):
    def __init__(self, project: str, directory_path: str) -> None:

        super().__init__()

        self.directory_path = os.path.abspath(directory_path)
        self.directory_path = os.path.join(self.directory_path, project)
        self.project = project

        os.makedirs(self.directory_path, exist_ok=True)

    def save_table_manifest(self, tables: List[str]):

        path = os.path.join(self.directory_path, f"{self.project}_tables.yml")
        tables = [{"table": t} for t in tables]
        data = {"database": self.project, "tables": tables}
        data_yml = yaml.safe_dump(data)
        with open(path, mode="w", encoding="utf-8") as f:
            f.write(data_yml)

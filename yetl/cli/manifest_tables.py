import os
import glob
import re


class Source:
    def __init__(self) -> None:
        pass

    def tables(self) -> list:
        return []

    def table_columns(self, table_name: str) -> list:
        return []


class FileSource(Source):
    def __init__(
        self, directory_path: str, filename: str = "*", name_mask: str = None
    ) -> None:
        self.directory_path = directory_path
        self.filename = filename
        self.name_mask = name_mask

    def _extract_name(self, name: str):
        try:
            result = re.search(self.name_mask, name).group()
            return result
        except:
            return None

    def tables(self) -> list:
        files_found = []
        for root, dirs, _ in os.walk(self.directory_path):
            for d in dirs:
                path = os.path.join(root, d, self.filename)
                dir_files = glob.glob(path)
                dir_files = [os.path.basename(df) for df in dir_files]
                if self.name_mask:
                    dir_files = [
                        self._extract_name(df)
                        for df in dir_files
                        if self._extract_name(df)
                    ]
                files_found = files_found + dir_files

        # de-dupe names
        file_set = set()
        file_set_add = file_set.add
        files_found = [x for x in files_found if not (x in file_set or file_set_add(x))]

        return files_found

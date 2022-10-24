class SchemaNotFound(Exception):
    def __init__(self, path: str):
        self.path = path
        super().__init__(self.path)

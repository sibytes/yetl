from abc import ABC


class Save(ABC):
    def __init__(self, dataset) -> None:
        self.dataset = dataset

    def write(self):
        self.dataset.context.log.info(
            f"Writer saving using the {self.__class__.__name__} "
        )

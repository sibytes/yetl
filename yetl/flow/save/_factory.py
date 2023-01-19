from ._saves import (
    Save,
    AppendSave,
    IgnoreSave,
    OverwriteSave,
    OverwriteSchemaSave,
    ErrorIfExistsSave,
    MergeSave,
)

# import logging
from ..dataset import Destination
from ._save_mode_type import SaveModeOptions
import logging


class _SaveFactory:
    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._save = {}

    def register_save_type(self, save_mode_type: SaveModeOptions, save_type: type):
        self._logger.debug(f"Register dataset type {save_type} as {type}")
        self._save[save_mode_type] = save_type

    def get_save_type(self, dataset: Destination, options: dict = None) -> Save:

        type: SaveModeOptions = dataset.write.mode

        self._logger.debug(f"Get {type.name} from factory save")
        save_class = self._save.get(type)

        if not save_class:
            self._logger.error(
                f"SaveModeOptions {type.name} not registered in the save factory"
            )
            raise ValueError(type)

        # TODO: any constructor args that needed
        # options["dataset"] = dataset
        options = {} if options is None else options
        return save_class(dataset=dataset, **options)


factory = _SaveFactory()
factory.register_save_type(SaveModeOptions.APPEND, AppendSave)
factory.register_save_type(SaveModeOptions.IGNORE, IgnoreSave)
factory.register_save_type(SaveModeOptions.OVERWRITE, OverwriteSave)
factory.register_save_type(SaveModeOptions.OVERWRITE_SCHEMA, OverwriteSchemaSave)
factory.register_save_type(SaveModeOptions.ERROR_IF_EXISTS, ErrorIfExistsSave)
factory.register_save_type(SaveModeOptions.MERGE, MergeSave)

from ._saves import (
    Save,
    AppendSave,
    IgnoreSave,
    OverwriteSave,
    OverwriteSchemaSave,
    ErrorIfExistsSave,
    MergeSave,
    # DefaultSave,
)
import logging
from ..dataset import Destination
from ._save_mode_type import SaveModeType


class _SaveFactory:
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)
        self._save = {}

    def register_save_type(self, save_mode_type: SaveModeType, save_type: type):
        self._logger.debug(f"Register dataset type {save_type} as {type}")
        self._save[save_mode_type] = save_type

    def get_save_type(self, dataset: Destination, options:dict={}) -> Save:

        type: SaveModeType = dataset.write.get_mode()

        self._logger.info(f"Get {type.name} from factory save")
        save_class = self._save.get(type)

        if not save_class:
            self._logger.error(
                f"SaveModeType {type.name} not registered in the save factory"
            )
            raise ValueError(type)

        # TODO: any constructor args that needed
        options["dataset"] = dataset
        return save_class(**options)


factory = _SaveFactory()
# factory.register_save_type(SaveModeType.DEFAULT, DefaultSave)
factory.register_save_type(SaveModeType.APPEND, AppendSave)
factory.register_save_type(SaveModeType.IGNORE, IgnoreSave)
factory.register_save_type(SaveModeType.OVERWRITE, OverwriteSave)
factory.register_save_type(SaveModeType.OVERWRITE_SCHEMA, OverwriteSchemaSave)
factory.register_save_type(SaveModeType.ERROR_IF_EXISTS, ErrorIfExistsSave)
factory.register_save_type(SaveModeType.MERGE, MergeSave)

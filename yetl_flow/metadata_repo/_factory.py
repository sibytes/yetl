from enum import Enum

from ._metadata_deltalake import MetadataDeltalake
from ._metadata_file import MetadataFile
from ._imetadata_repo import IMetadataRepo
import logging


class MetadataRepoType(Enum):
    METADATA_FILE = 1
    METADATA_DATALAKE = 2


class _MetadataRepoFactory:
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)
        self._metadata_repo = {}

    def register_metadata_repo_type(
        self, sr_type: MetadataRepoType, metadata_repo_type: type
    ):
        self._logger.debug(
            f"Register metadata repo type {metadata_repo_type} as {type}"
        )
        self._metadata_repo[sr_type] = metadata_repo_type

    def _get_sr_type(self, name: str):
        try:
            if MetadataRepoType[name] in MetadataRepoType:
                return MetadataRepoType[name]
        except:
            return None

    def get_metadata_repo_type(self, context, config: dict) -> IMetadataRepo:

        metadata_repo_store: str = next(iter(config))
        mr_type: MetadataRepoType = self._get_sr_type(metadata_repo_store)

        context.log.info(f"Setting up metadata repo on {metadata_repo_store} ")

        context.log.debug(f"Setting MetadataRepoType using type {mr_type}")
        metadata_repo: IMetadataRepo = self._metadata_repo.get(mr_type)

        if not metadata_repo:
            self._logger.error(
                f"MetadataRepoType {mr_type.name} not registered in the metadata_repo factory"
            )
            raise ValueError(mr_type)

        return metadata_repo(context, config)


factory = _MetadataRepoFactory()
factory.register_metadata_repo_type(MetadataRepoType.METADATA_FILE, MetadataFile)
factory.register_metadata_repo_type(
    MetadataRepoType.METADATA_DATALAKE, MetadataDeltalake
)

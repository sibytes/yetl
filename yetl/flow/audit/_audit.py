from enum import Enum
from datetime import datetime
import json
from uuid import UUID
import yaml
import time
from ..parser.parser import reduce_whitespace
from ..warnings import Warning
from pydantic import BaseModel, Field, PrivateAttr
from typing import Union, Any, Dict
import logging


class AuditLevel(Enum):
    DATAFLOW = "dataflow"
    DATASETS = "datasets"
    WARNING = "warning"
    ERROR = "error"


class AuditFormat(Enum):
    JSON = "json"
    YAML = "yaml"


class AuditTask(Enum):
    SQL = "sql"
    SET_TABLE_PROPERTIES = "set_table_properties"
    GET_TABLE_PROPERTIES = "get_table_properties"
    DELTA_TABLE_WRITE = "delta_table_write"
    DELTA_TABLE_OPTIMIZE = "delta_table_optimize"
    SCHEMA_ON_READ_VALIDATION = "schema_on_read_validation"
    LAZY_READ = "lazy_read"


class Audit(BaseModel):
    _COUNT = "count"

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)

        self.audit_log = {
            AuditLevel.DATAFLOW.value: {AuditLevel.DATASETS.value: {}},
            AuditLevel.WARNING.value: {self._COUNT: 0},
            AuditLevel.ERROR.value: {self._COUNT: 0},
        }

    audit_log: Dict[str, dict] = Field(default=None)
    _task_counter: dict = PrivateAttr(default={})
    _logger: Any = PrivateAttr(default=None)

    def error(self, exception: Exception):
        self._logger.exception(exception)
        data = {"exception": exception.__class__.__name__, "message": str(exception)}
        self._append(data, AuditLevel.ERROR)

    def warning(self, warning: Warning):
        self._logger.warning(str(warning))
        data = {"warning": warning.__class__.__name__, "message": str(warning)}
        self._append(data, AuditLevel.WARNING)

    def dataset_task(
        self,
        dataset_id: UUID,
        task: AuditTask,
        detail: Union[str, dict],
        start_datetime: datetime,
    ):

        end_datetime = datetime.now()
        duration = (end_datetime - start_datetime).total_seconds()

        if isinstance(detail, str):
            detail = reduce_whitespace(detail)

        audit_step = {
            "task": task.value,
            "message": detail,
            "start_datetime": start_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "end_datetime": end_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "seconds_duration": duration,
        }
        self._logger.info(detail)

        data = {self._next_task_id(dataset_id): audit_step}
        if self.audit_log[AuditLevel.DATAFLOW.value][AuditLevel.DATASETS.value][
            str(dataset_id)
        ].get("tasks"):
            self.audit_log[AuditLevel.DATAFLOW.value][AuditLevel.DATASETS.value][
                str(dataset_id)
            ]["tasks"].update(data)
        else:
            self.audit_log[AuditLevel.DATAFLOW.value][AuditLevel.DATASETS.value][
                str(dataset_id)
            ] |= {"tasks": data}

    def dataset(self, data: dict):
        self._logger.info(yaml.safe_dump(data))
        self.audit_log[AuditLevel.DATAFLOW.value][AuditLevel.DATASETS.value] |= data

    def dataflow(self, data: dict):
        self._logger.info(yaml.safe_dump(data))
        self._append(data, AuditLevel.DATAFLOW)

    def save(self, data: dict):
        pass

    def _append(self, data: dict, level: AuditLevel):
        self.audit_log[level.value] |= data
        self._increment_count(level)

    def _increment_count(self, level: AuditLevel):
        if isinstance(self.audit_log[level.value].get(self._COUNT), int):
            self.audit_log[level.value][self._COUNT] += 1

    def get(self, format: AuditFormat = AuditFormat.JSON):

        if format == AuditFormat.JSON:
            metadata = json.dumps(self.audit_log, indent=4, default=str)
        elif format == AuditFormat.YAML:
            metadata = yaml.safe_dump(self.audit_log, indent=4)
        else:
            metadata = json.dumps(self.audit_log, indent=4, default=str)

        return metadata

    def _next_task_id(self, dataset_id: UUID) -> int:

        if dataset_id in self._task_counter.keys():
            self._task_counter[dataset_id] += 1
        else:
            self._task_counter[dataset_id] = 0

        return self._task_counter[dataset_id]

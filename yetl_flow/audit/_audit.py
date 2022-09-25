from curses import meta
from enum import Enum
from datetime import datetime
import json
from uuid import UUID
import yaml
import time
from ..parser.parser import reduce_whitespace
from ..warnings import Warning

class AuditLevel(Enum):
    DATAFLOW = "dataflow"
    WARNING = "warning"
    ERROR = "error"


class AuditFormat(Enum):
    JSON = "json"
    YAML = "yaml"

class AuditTask(Enum):
    SQL = "sql"



class Audit:
    _COUNT = "count"

    def __init__(self) -> None:
        self.audit_log = {
            AuditLevel.DATAFLOW.value: {},
            AuditLevel.WARNING.value: {self._COUNT: 0},
            AuditLevel.ERROR.value: {self._COUNT: 0},
        }
        self._task_counter = {}

    def error(self, exception: Exception):
        data = {
            "exception": exception.__class__.__name__, 
            "message": str(exception)
        }
        self._append(data, AuditLevel.ERROR)

    def warning(self, warning:Warning):
        data = {
            "warning": warning.__class__.__name__, 
            "message": str(warning)
        }
        self._append(data, AuditLevel.WARNING)

    def dataflow_task(self, dataset_id:UUID, task:AuditTask, detail:str, start_datetime:datetime):

        end_datetime = datetime.now()
        duration = (end_datetime-start_datetime).total_seconds()

        audit_step = {
                "task": task.value,
                "message": reduce_whitespace(detail),
                "start_datetime": start_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "end_datetime": end_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "seconds_duration": duration
        }

        data = {self._next_task_id(dataset_id): audit_step}
        if self.audit_log[AuditLevel.DATAFLOW.value][str(dataset_id)].get("steps"):
            self.audit_log[AuditLevel.DATAFLOW.value][str(dataset_id)]["steps"] |= data
        else:
            self.audit_log[AuditLevel.DATAFLOW.value][str(dataset_id)] |= {"steps": data}


    def dataflow(self, data: dict):
        self._append(data, AuditLevel.DATAFLOW)

    def save(self, data: dict):
        pass

    def _append(self, data: dict, level: AuditLevel):
        self.audit_log[level.value] |= data
        self._increment_count(level)

    def _increment_count(self, level: AuditLevel):
        if self.audit_log[level.value].get(self._COUNT):
            self.audit_log[level.value][self._COUNT] = +1

    def get(self, format: AuditFormat = AuditFormat.JSON):

        match format:
            case AuditFormat.JSON:
                metadata = json.dumps(self.audit_log, indent=4, default=str)
            case AuditFormat.YAML:
                metadata = yaml.safe_dump(self.audit_log, indent=4)
            case _:
                metadata = json.dumps(self.audit_log, indent=4, default=str)

        return metadata

    def _next_task_id(self, dataset_id:UUID)->int:

        if dataset_id in self._task_counter.keys():
            self._task_counter[dataset_id] =+ 1
        else:
            self._task_counter[dataset_id] = 0

        return self._task_counter[dataset_id]
        

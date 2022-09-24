from curses import meta
from enum import Enum
from datetime import datetime
import json
from xml.dom.pulldom import default_bufsize
import yaml


class AuditLevel(Enum):
    DATAFLOW = "dataflow"
    WARNING = "warning"
    ERROR = "error"


class AuditFormat(Enum):
    JSON = "json"
    YAML = "yaml"


class Audit:
    _COUNT = "count"

    def __init__(self) -> None:
        self.audit_log = {
            AuditLevel.DATAFLOW.value: {},
            AuditLevel.WARNING.value: {self._COUNT: 0},
            AuditLevel.ERROR.value: {self._COUNT: 0},
        }

    def error(self, data: dict):
        self._append(data, AuditLevel.WARNING)

    def warning(self, data: dict):
        self._append(data, AuditLevel.WARNING)

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

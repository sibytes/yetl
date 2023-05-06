from enum import Enum


class StageType(str, Enum):
    audit_control = "audit_control"
    landing = "landing"
    raw = "raw"
    base = "base"
    curated = "curated"
    extract = "extract"

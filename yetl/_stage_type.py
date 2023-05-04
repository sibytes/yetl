from enum import Enum


class StageType(str, Enum):
    landing = "landing"
    raw = "raw"
    base = "base"
    curated = "curated"
    extract = "extract"

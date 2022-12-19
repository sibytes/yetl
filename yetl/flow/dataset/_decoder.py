from ..parser._constants import YetlTableProperties
from typing import Dict


def _parse_name(name: str):
    """Converts a string class property name
    to a string of yetl config property name

    Given namepart1_name_part2
    Returns yetl.namepart1.namePart2
    """

    name_parts = name.split("_")
    part_1 = name_parts[0]
    name_parts = [name_parts[1]] + [n.capitalize() for n in name_parts[2:]]
    part_2 = "".join(name_parts)
    # ensures that it's a valid yetl property
    config_name = f"yetl.{part_1}.{part_2}"
    try:
        config_name = YetlTableProperties(config_name).value
    except ValueError as e:
        msg = f"An error occured decoding yetl properties in pydantic from {name} to {config_name}. {e}"
        raise Exception(msg) from e

    return config_name


def parse_properties(obj: dict):
    obj = {_parse_name(k): obj for k, obj in obj.items()}
    return obj


def parse_properties_key(key: str):
    return "properties" if key == "yetl_properties" else key


def parse_properties_values(key: str, value: Dict[str, str]):
    if key == "yetl_properties":
        value = parse_properties(value)
    return value

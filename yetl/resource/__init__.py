try:
    from importlib.resources import files as resources
except Exception:
    from importlib import resources

_PACKAGE = "yetl.resource"


def get_resource_text(resource: str):
    try:
        data = resources(_PACKAGE).joinpath(resource).read_text()
    except Exception:
        data = resources.read_text(_PACKAGE, resource)
    return data


def get_resource_binary(resource: str):
    try:
        schema = resources(_PACKAGE).joinpath(resource).read_bytes()
    except Exception:
        schema = resources.read_binary(_PACKAGE, resource)
    return schema

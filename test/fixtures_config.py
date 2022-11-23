import pytest
import yaml
import os


def load_config(name: str) -> dict:
    path = os.path.curdir
    path = os.path.abspath(path)
    path = os.path.join(path, "test", "config", name)
    path = f"{path}.yaml"
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


@pytest.fixture
def i_context_config():
    return load_config("i_context")

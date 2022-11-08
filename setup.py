import pathlib
from setuptools import setup, find_packages


# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="yetl-framework",
    version="0.0.25.dev2",
    description="yet (another spark) etl framework",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://www.yetl.io/",
    project_urls={
        "GitHub": "https://github.com/sibytes/yetl",
        "Documentation": "https://www.yetl.io/",
    },
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ],
    packages=[
        "yetl",
        "yetl.cli",
        "yetl.flow",
        "yetl.flow.audit",
        "yetl.flow.context",
        "yetl.flow.dataflow",
        "yetl.flow.dataset",
        "yetl.flow.exceptions",
        "yetl.flow.file_system",
        "yetl.flow.metadata_repo",
        "yetl.flow.parser",
        "yetl.flow.save",
        "yetl.flow.schema_repo",
        "yetl.flow.warnings",
        "yetl.workflow",
    ],
    install_requires=["pyyaml", "pyspark", "regex", "delta-spark", "Jinja2", "typer"],
    zip_safe=False,
)

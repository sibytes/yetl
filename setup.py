import pathlib
from setuptools import setup, find_packages


# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="yetl-framework",
    version="0.0.17",
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
        "Programming Language :: Python :: 3.8",
    ],
    packages=[
        "yetl_flow",
        "yetl_flow.audit",
        "yetl_flow.context",
        "yetl_flow.dataflow",
        "yetl_flow.dataset",
        "yetl_flow.exceptions",
        "yetl_flow.file_system",
        "yetl_flow.metadata_model",
        "yetl_flow.metadata_repo",
        "yetl_flow.parser",
        "yetl_flow.schema_repo",
        "yetl_flow.warnings",
    ],
    install_requires=["pyyaml", "pyspark", "regex", "delta-spark"],
    zip_safe=False,
)

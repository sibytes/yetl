import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="yetl-framework",
    version="1.2.12",
    description="yet (another spark) etl framework",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://www.yetl.io/",
    project_urls={
        "GitHub": "https://github.com/sibytes/yetl",
        "Documentation": "https://www.yetl.io/",
    },
    author="Shaun Ryan",
    author_email="shaun_chiburi@hotmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    packages=[
        "yetl",
        "yetl.workflow",
        "yetl.table"
    ],
    install_requires=[
          'PyYAML',
          'jinja2',
          'pydantic'
      ],
    zip_safe=False
)

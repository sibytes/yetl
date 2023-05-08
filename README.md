# yetl

Website: https://www.yetl.io/

# Introduction

Install
```
pip install yetl-framework
```

Configuration framework for databricks pipelines.
Define configuration and table dependencies in yaml config then get the table mappings config model:

Define your tables.

```yaml
audit_control:
  delta_lake:
    raw_dbx_patterns_control:
      header_footer:
        sql: ../sql/{{database}}/{{table}}.sql
        depends_on:
          - raw.raw_dbx_patterns.*
      raw_audit:
        sql: ../sql/{{database}}/{{table}}.sql
        depends_on:
          - raw.raw_dbx_patterns.*
          - audit_control.raw_dbx_patterns_control.header_footer

landing:
  read:
    landing_dbx_patterns:
      customer_details_1: null
      customer_details_2: null

raw:
  delta_lake:
    raw_dbx_patterns:
      customers:
        ids: id
        depends_on:
          - landing.landing_dbx_patterns.customer_details_1
          - landing.landing_dbx_patterns.customer_details_2
        warning_thresholds:
          invalid_ratio: 0.1
          invalid_rows: 0
          max_rows: 100
          min_rows: 5
        exception_thresholds:
          invalid_ratio: 0.2
          invalid_rows: 2
          max_rows: 1000
          min_rows: 0
        custom_properties:
          process_group: 1
```

Define you load configuration:

```yaml
version: 1.0.0
tables: ./tables.yaml

audit_control:
  delta_lake:
    # delta table properties can be set at stage level or table level
    delta_properties:
        delta.appendOnly: true
        delta.autoOptimize.autoCompact: true
        delta.autoOptimize.optimizeWrite: true
    managed: false
    create_table: true
    container: datalake
    location: /mnt/{{container}}/data/raw
    checkpoint_location: "/mnt/{{container}}/checkpoint/{{checkpoint}}"
    path: "{{database}}/{{table}}"
    options:
      checkpointLocation: default

landing:
  read:
    trigger: customerdetailscomplete-{{filename_date_format}}*.flg
    trigger_type: file
    container: datalake
    location: "/mnt/{{container}}/data/landing/dbx_patterns/{{table}}/{{path_date_format}}"
    filename: "{{table}}-{{filename_date_format}}*.csv"
    filename_date_format: "%Y%m%d"
    path_date_format: "%Y%m%d"
    format: cloudFiles
    spark_schema: ../schema/{{table.lower()}}.yaml
    options:
      # autoloader
      cloudFiles.format: csv
      cloudFiles.schemaLocation:  /mnt/{{container}}/checkpoint/{{checkpoint}}
      cloudFiles.useIncrementalListing: auto
      # schema
      inferSchema: false
      enforceSchema: true
      columnNameOfCorruptRecord: _corrupt_record
      # csv
      header: false
      mode: PERMISSIVE
      encoding: windows-1252
      delimiter: ","
      escape: '"'
      nullValue: ""
      quote: '"'
      emptyValue: ""
    
raw:
  delta_lake:
    # delta table properties can be set at stage level or table level
    delta_properties:
      delta.appendOnly: true
      delta.autoOptimize.autoCompact: true    
      delta.autoOptimize.optimizeWrite: true  
      delta.enableChangeDataFeed: false
    managed: false
    create_table: true
    container: datalake
    location: /mnt/{{container}}/data/raw
    path: "{{database}}/{{table}}"
    checkpoint_location: "/mnt/{{container}}/checkpoint/{{checkpoint}}"
    options:
      mergeSchema: true

base:
  delta_lake:
    container: datalake
    location: /mnt/{{container}}/data/base
    path: "{{database}}/{{table}}"
    options: null
```

Import the config objects into you pipeline:

```python
from yetl import Config, StageType

pipeline = "auto_load_schema"
project = "test_project"
config = Config(
    project=project, pipeline=pipeline
)
table_mapping = config.get_table_mapping(
    stage=StageType.raw, table="customers"
)

print(table_mapping)
```

Use even less code and use the decorator pattern:

```python
@yetl_flow(
        project="test_project", 
        stage=StageType.raw
)
def auto_load_schema(table_mapping:TableMapping):

    # << ADD YOUR PIPELINE LOGIC HERE - USING TABLE MAPPING CONFIG >>
    return table_mapping # return whatever you want here.


result = auto_load_schema(table="customers")
```

## Development Setup

```
pip install -r requirements.txt
```

## Unit Tests

To run the unit tests with a coverage report.

```
pip install -e .
pytest test/unit --junitxml=junit/test-results.xml --cov=yetl --cov-report=xml --cov-report=html
```

## Integration Tests

To run the integration tests with a coverage report.

```
pip install -e .
pytest test/integration --junitxml=junit/test-results.xml --cov=yetl --cov-report=xml --cov-report=html
```

## Build

```
python setup.py sdist bdist_wheel
```

## Publish


```
twine upload dist/*
```

# yetl

Configuration framework for databricks pipelines.
Define configuration and table dependencies in yaml config then get the table mappings config model:

Define your tables.

```yaml

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

base:
  delta_lake:
    # delta table properties can be set at stage level or table level
    delta_properties:
      delta.appendOnly: true
      delta.autoOptimize.autoCompact: true    
      delta.autoOptimize.optimizeWrite: true  
      delta.enableChangeDataFeed: false
    base_dbx_patterns:
      customer_details_1:
        ids: id
        depends_on:
          - raw.raw_dbx_patterns.customers
        # delta table properties can be set at stage level or table level
        # table level properties will overwride stage level properties
        delta_properties:
            delta.enableChangeDataFeed: true
      customer_details_2:
        ids: id
        depends_on:
          - raw.raw_dbx_patterns.customers
```

Define you load configuration:

```yaml
version: 1.0.0
tables: ./tables.yaml

landing:
  read:
    trigger: customerdetailscomplete-{{filename_date_format}}*.flg
    trigger_type: file
    container: datalake
    root: "/mnt/{{container}}/data/landing/dbx_patterns/{{table}}/{{path_date_format}}"
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
    root: /mnt/{{container}}/data/raw
    path: "{{database}}/{{table}}"
    options:
      checkpointLocation: /mnt/{{container}}/checkpoint/{{database}}_{{table}}
      mergeSchema: true
```

Import the config objects into you pipeline:

```python
from yetl import Config, Timeslice, StageType

# build path to configuration file
pipeline = "auto_load_schema"
project = "test_project"
timeslice = Timeslice(day="*", month="*", year="*")
config = Config(
    project=project, pipeline=pipeline
)
table_mapping = config.get_table_mapping(
    timeslice=timeslice, stage=StageType.raw, table="customers"
)

print(table_mapping)
```

Use even less code and use the decorator pattern:

```python
@yetl_flow(
        project="test_project", 
        stage=StageType.raw, 
        config_path="./test/config"
)
def auto_load_schema(table_mapping:TableMapping):
    return table_mapping


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

## Build

```
python setup.py sdist bdist_wheel
```

## Publish


```
twine upload dist/*
```

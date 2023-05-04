# dbxconfig

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
tables: ./tables.yaml

landing:
  read:
    trigger: customerdetailscomplete-{{filename_date_format}}*.flg
    trigger_type: file
    database: landing_dbx_patterns
    table: "{{table}}"
    container: datalake
    root: "/mnt/{{container}}/data/landing/dbx_patterns/{{table}}/{{path_date_format}}"
    filename: "{{table}}-{{filename_date_format}}*.csv"
    filename_date_format: "%Y%m%d"
    path_date_format: "%Y%m%d"
    format: cloudFiles
    spark_schema: ../Schema/{{table.lower()}}.yaml
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
    database: raw_dbx_patterns
    table: "{{table}}"
    container: datalake
    root: /mnt/{{container}}/data/raw
    path: "{{database}}/{{table}}"
    options:
      checkpointLocation: /mnt/{{container}}/checkpoint/{{database}}_{{table}}
      mergeSchema: true
```

Import the config objects into you pipeline:

```python
from dbxconfig import Config, Timeslice, StageType

# build path to configuration file
pattern = "auto_load_schema"
config_path = f"../Config"

# create a timeslice object for slice loading. Use * for all time (supports hrs, mins, seconds and sub-second).
timeslice = Timeslice(day="*", month="*", year="*")

# parse and create a config objects
config = Config(config_path=config_path, pattern=pattern)

# get the configuration for a table mapping to load.
table_mapping = config.get_table_mapping(
    timeslice=timeslice, 
    stage=StageType.raw, 
    table="customers"
)

print(table_mapping)
```

## Development Setup

```
pip install -r requirements.txt
```

## Unit Tests

To run the unit tests with a coverage report.

```
pip install -e .
pytest test/unit --junitxml=junit/test-results.xml --cov=dbxconfig --cov-report=xml --cov-report=html
```

## Build

```
python setup.py sdist bdist_wheel
```

## Publish


```
twine upload dist/*
```

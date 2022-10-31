<img src="https://img.shields.io/badge/Python-v3.8-blue">

# YETL

`pip install yetl-framework`

Website & Docs: [Yet (another Apache Spark) ETL Framework](https://www.yetl.io/)


Example:

## Define a dataflow

```python

from yetl.flow import (
    yetl_flow, 
    IDataflow, 
    IContext, 
    Timeslice, 
    TimesliceUtcNow, 
    OverwriteSave, 
    Save
)
from pyspark.sql.functions import *
from typing import Type

@yetl_flow(project="demo")
def batch_text_csv_to_delta_permissive_1(
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load the demo customer data as is into a raw delta hive registered table.

        the config for this dataflow has 2 landing sources that are joined
        and written to delta table
        delta tables are automatically created and if configured schema exceptions
        are loaded syphened into a schema exception table
    """

    df_cust = dataflow.source_df(f"{context.project}_landing.customer")
    df_prefs = dataflow.source_df(f"{context.project}_landing.customer_preferences")

    context.log.info("Joining customers with customer_preferences")
    df = df_cust.join(df_prefs, "id", "inner")
    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )

    dataflow.destination_df(f"{context.project}_raw.customer", df, save=save)
```

## Run an incremental load:

```python
timeslice = Timeslice(2022, 7, 12)
results = batch_text_csv_to_delta_permissive_1(
    timeslice = Timeslice(2022, 7, 12)
)
```

## Run a full load for Year 2022:

```python
results = batch_text_csv_to_delta_permissive_1(
    timeslice = Timeslice(2022, '*', '*'),
    save = OverwriteSave
)
```

## Dependencies & Setup

This is a spark application with DeltaLake it requires following dependencies installed in order to run locally:
- [Java Runtime 11](https://openjdk.org/install/)
- [Apache Spark 3.2.2 hadoop3.2](https://spark.apache.org/downloads.html)

Ensure that the spark home path and is added to youy path is set Eg:
```
export SPARK_HOME="$HOME/opt/spark-3.2.2-bin-hadoop3.3"
```

Enable DeltaLake by:
```
cp $SPARK_HOME/conf/spark-defaults.conf.template  $SPARK_HOME/conf/spark-defaults.conf
```
Add the following to `spark-defaults.conf`:
```
spark.jars.packages               io.delta:delta-core_2.12:2.0.0
spark.sql.extensions              io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog   org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.sql.catalogImplementation   hive
```

## Python Project Setup

Create virual environment and install dependencies for local development:

```
python -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install --editable .
```


## Build

Build python wheel:

```
python setup.py sdist bdist_wheel
```

There is a CI build configured for this repo that builds on main origin and publishes to PyPi.


# Releases

Version: 0.0.23
- Added metadata lineage configuration into sources and destinations for context, dataflow and dataset id's
- Removed spark logging argument from decorator to the config since allows context to be more abstract and is less confusing.
- Added workflow module and prototype for multithreaded loading

Version: 0.0.22
- Introduced YETL optimize table property since there are still reasons to optimise on databricks
- Regression tested/fixes SQL Reader
- Fixed bug that was missing lineage columns off automatic schema table creation.
- Adjusted table creation on delta writer so that when schema is inferred the table is created afterwards to avoid schema partition synchronisation errors
- Added in configuration for putting file origin into the source dataframes
- Added configuration to add _corrupt_record on schema creation
- Auto generating sql schema's on schema creation
- Adding options for dynamic template loading from a single function so that it can be re-used across tables
- deprecated custom parsing timeslice features in favour of jinja templating
- Added in a new cli lib for build out templates and maintenance tasks using typer

Version: 0.0.21
- Added in Jinja for variables replacements as more robust solution than simple string replacements.


Version: 0.0.20
- Fixed missing packages in build

Version: 0.0.19
- Fixed missing packages in build

Version: 0.0.18

- Major cleanup and refactor of datasets for future road map
- Sources and destinations have the same auto_io lifecycle in the dataflow, auto is called on retrieval from the dataflow collections
- Added SQLReader dataset type so we can define SQL Sources from any hive table in data flows that write to destinations (e.g. delta lake tables)
- Fixed audit error trapping


Version: 0.0.17

- Integration testing with databricks.
- Refactored configuration so that there is more re-use across environments
- Dataset types are now specifically declared in the configuration to reduce complexity when adding more types of datasets.

Version: 0.0.16

- Refactored context into inteface to allow the future expansion into engines other than spark.

Version: 0.0.15

- raise errors and warnings from thresholds configurations
- Refactored audit loging and added comprehensive data flow auditing.

Version: 0.0.14

- Started building in integration tests
- Refactored Destination save using class composition
- Recfactored save dependency injection down to the dataset level
- Added support for Merge save using deltalake

Version: 0.0.13

- Added support default schema creation etl.schema.createIfNotExists.
- refactoed and cleaned up the basic reader
- added consistent validation and consistent property settings to basic reader
- added reader skipping features based on configuration settings

Version: 0.0.12

- Added support multicolumn zording.

Version: 0.0.11

- [Upgrade to spark 3.3](https://github.com/sibytes/yetl/issues/32). Upgraded development for spark 3.3 and delta lake 2.1.
- Added _timeslice metadata column parsing into the destination dataset so that it can be used for partitioning, works even if the read path is wildcarded '*'
- Added support for partition based optimization on writes
- Added support for multi column partitioning

Version: 0.0.10

- [Fix YAML Schema Format Error when Dataflow Retries are Set to 0](https://github.com/orgs/sibytes/projects/2/views/1). Fixed dictionary extraction bug for setting retries and retry_wait to zero.
- Added overwrite schema save
- Added partition sql support
- Fixed constraints synchronisation to drop and create more efficiently
- Refined, refactored and fixed lineage
- Added file lineage logging
- Add file lineage logging
- Detect spark and databricks versions, determine whether to auto optimise and compact


Version: 0.0.9

- [Clean Up BadRecords JSON Files](https://github.com/orgs/sibytes/projects/2) Automatically remove json schema exception files created by the BadRecordsPath exception handler after they are loaded into a delta table.

Version: 0.0.8

- Including all packages in distribution.

Version: 0.0.7

- [Fix the Timeslice on Wildcard Loads](https://github.com/sibytes/yetl/issues/37) - wildcard format not working on databricks. Inserting %* instead of *.
- Yetl CDC Pattern example and tests

Version: 0.0.6

-  [Fix Reader Bad Records](https://github.com/sibytes/yetl/issues/1) - Support exceptions handling for badrecordspath defined in the configuration e.g. [landing.customer.read.badRecordsPath](https://github.com/sibytes/yetl/blob/main/config/pipeline/dbx_dev/customer_landing_to_rawdb_csv.yaml). Only supported in databricks runtime environment.
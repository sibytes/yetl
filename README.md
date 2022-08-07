<img src="https://img.shields.io/badge/Python-v3.8-blue">

# YETL

`pip install yetl-framework`

Website & Docs: [Yet (another Apache Spark) ETL Framework](https://www.yetl.io/)


Example:

## Define a dataflow

```python

from yetl_flow import (
    yetl_flow, 
    IDataflow, 
    Context, 
    Timeslice, 
    TimesliceUtcNow, 
    OverwriteSave, 
    Save
)
from pyspark.sql.functions import *
from typing import Type

@yetl_flow(log_level="ERROR")
def customer_landing_to_rawdb_csv(
    context: Context, 
    dataflow: IDataflow, 
    timeslice: Timeslice = TimesliceUtcNow(), 
    save_type: Type[Save] = None
) -> dict:
    """Load the demo customer data as is into a raw delta hive registered table."""

    # the config for this dataflow has 2 landing sources that are joined
    # and written to delta table
    # delta tables are automatically created and if configured schema exceptions
    # are loaded syphened into a schema exception table
    df_cust = dataflow.source_df("landing.customer")
    df_prefs = dataflow.source_df("landing.customer_preferences")

    context.log.info("Joining customers with customer_preferences")
    df = df_cust.join(df_prefs, "id", "inner")
    df = df_cust

    dataflow.destination_df("raw.customer", df)
```

## Run an incremental load:

```python
timeslice = Timeslice(2022, 7, 12)
results = customer_landing_to_rawdb_csv(
    timeslice = Timeslice(2022, 7, 12)
)
```

## Run a full load:

```python
results = customer_landing_to_rawdb_csv(
    timeslice = Timeslice(2022, '*', '*'),
    save_type = OverwriteSave
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


# Change Log

Version: 0.0.6

-  [Fix Reader Bad Records](https://github.com/sibytes/yetl/issues/1) - Support exceptions handling for badrecordspath defined in the configuration e.g. [landing.customer.read.badRecordsPath](https://github.com/sibytes/yetl/blob/main/config/pipeline/dbx_dev/customer_landing_to_rawdb_csv.yaml). Only supported in databricks runtime environment.

Version: 0.0.8 - WIP

- [Fix the Timeslice on Wildcard Loads](https://github.com/sibytes/yetl/issues/37) - wildcard format not working on databricks. Inserting %* instead of *.
- Yetl CDC Pattern example and tests
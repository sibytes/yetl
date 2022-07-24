<img src="https://img.shields.io/badge/Python-v3.8-blue">

# Messing About with Delta Lake Open Source

The configuration is held in `./config` see the configuration section below.

This is test rig for messing about the open source delta lake locally with a local installation on spark.
This is mac setup with vscode. Windows may not work with these instructions.

## Dependencies & Setup

This is a spark application with DeltaLake it requires following dependencies installed in order to run locally:
- [Java Runtime 11]()
- [Apache Spark 2.3.1 hadoop3.2](spark-3.2.1-bin-hadoop3.2)

Ensure that the spark home path and is added to youy path is set Eg:
```
export SPARK_HOME="$HOME/opt/spark-3.2.1-bin-hadoop3.2"
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

## Running

It can be executed in vscode by hitting F5 with the settings in .vscode.
It will load the data using spark into a raw delta table called `raw.customer` persisted into the hive metastore.

For the entry point see `main.py` that calls into the module `pipeline`

## Explore the Result

Run `pyspark` in the project root and explore the data in `raw.customer`
```
df = spark.sql("SHOW SCHEMAS")
df.show()
df = spark.sql("SELECT * FROM raw.customer")
df.show()
```

The landing source data can be found here, this is persisted in git:
```
./data/landing
```

The deltalake data can be found here, this is NOT persisted in git and is removed by cleanup:
```
./data/delta_lake
```

## Cleaning Up
The local environment setup can be restored to it's originating state by executing:
```
sh cleanup.sh
```

## Testing

Testing place holder for anything you might want to write tests for:
```
pytest
```

# Configuration

Load schema's are abstracted into config settings that can be found here `./config`:

- [config/schema/customer.yaml](config/schema/customer.yaml) is the source data spark schema

Load configurations are abstracted into config settings that can be found here `./config/[ENV]`. For example the `local` configuration:
- [config/pipeline/local/config.yaml](config/pipeline/local/config.yaml) is the general local configuration for the spark project
- [config/pipeline/local/customer_landing_to_raw.yaml](config/pipeline/local/customer_landing_to_raw.yaml) is the local pipeline configuration for loading raw

The ENV can be set in the [.env](.env) file for development and in an environment variable for operation:
```
ENVIRONMENT=local
```
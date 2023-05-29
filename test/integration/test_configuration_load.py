from yetl import Config, Timeslice, StageType, Read, DeltaLake, yetl_flow, TableMapping, ValidationThreshold
from yetl.config._project import SparkLoggingLevel 
from yetl.config.table import TableType
from yetl.config.table._read import SliceDateFormat
import pytest
import os
import shutil



@pytest.fixture()
def tear_down():
    def tear_down_fn():
        shutil.rmtree("./test/config/test_project/data", ignore_errors=True)
        shutil.rmtree("./metastore_db", ignore_errors=True)
        shutil.rmtree("./spark-warehouse", ignore_errors=True)
        try:
            os.remove("./derby.log")
        except Exception:
            pass
    return tear_down_fn

@pytest.fixture()
def root_path():

    root = os.path.abspath(os.getcwd())
    return root


def test_configuration_load(tear_down, root_path):
    tear_down()
    pipeline = "autoloader"
    config_path = "./test/config"
    project = "test_project"
    timeslice = Timeslice(day="*", month="*", year="*")
    config = Config(
        project=project, pipeline=pipeline, config_path=config_path, timeslice=timeslice
    )
    table_mapping = config.get_table_mapping(
        stage=StageType.raw, table="customers"
    )

    source: Read = table_mapping.source["customer_details_1"]
    destination: DeltaLake = table_mapping.destination
    config.set_checkpoint(source=source, destination=destination)

    assert source.table == "customer_details_1"
    assert destination.table == "customers"
    assert destination.stage == StageType.raw
    assert destination.database =='raw_dbx_patterns' 
    assert destination.table=='customers' 
    assert destination.id==[] 
    assert destination.custom_properties == {'process_group': 1} 
    assert destination.table_type == TableType.DeltaLake
    assert destination.warning_thresholds == ValidationThreshold(invalid_ratio=0.1, invalid_rows=0, max_rows=100, min_rows=5) 
    assert destination.exception_thresholds == ValidationThreshold(invalid_ratio=0.2, invalid_rows=2, max_rows=1000, min_rows=0)
    assert destination.project.config_path == f'{root_path}/test/config/test_project'
    assert destination.project.name == 'test_project'
    assert destination.project.sql == f'{root_path}/test/config/test_project/sql'
    assert destination.project.pipelines == f'{root_path}/test/config/test_project/pipelines'
    assert destination.project.databricks_notebooks == f'{root_path}/test/config/test_project/databricks/notebooks'
    assert destination.project.databricks_workflows == f'{root_path}/test/config/test_project/databricks/workflows'
    assert destination.project.databricks_queries == f'{root_path}/test/config/test_project/databricks/queries'
    assert destination.project.spark.config == {
        'spark.master': 'local', 'spark.databricks.delta.allowArbitraryProperties.enabled': 'True', 
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog', 
         'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension'
    }
    assert destination.project.spark.logging_level == SparkLoggingLevel.ERROR
    assert destination.container == 'datalake'
    assert destination.location == f'{root_path}/test/config/test_project/data/mnt/datalake/data/raw/raw_dbx_patterns/customers'
    assert destination.path == 'raw_dbx_patterns/customers'
    assert destination.options == {'mergeSchema': True, 'checkpointLocation': '/mnt/datalake/checkpoint/test_project/landing_dbx_patterns.customer_details_1-raw_dbx_patterns.customers'}
    assert destination.timeslice == Timeslice(year='*', month='*', day='*', hour=0, minute=0, second=0, microsecond=0)
    assert destination.checkpoint == 'landing_dbx_patterns.customer_details_1-raw_dbx_patterns.customers'
    assert destination.delta_constraints == None 
    assert destination.partition_by == None 
    assert destination.z_order_by == None 
    assert destination.managed == False 
    assert destination.sql == None
    assert source.slice_date == SliceDateFormat.FILENAME_DATE_FORMAT
    assert source.slice_date_column_name == "_slice_date"


def test_decorator_configuration_load(tear_down):
    @yetl_flow(
            project="test_project", 
            stage=StageType.raw, 
            config_path="./test/config"
    )
    def autoloader(table_mapping:TableMapping):
        return table_mapping
    

    result = autoloader(table="customers")
    tear_down()
    assert result.source["customer_details_1"].table == "customer_details_1"
    assert result.destination.table == "customers"


def test_decorator_configuration_audit_load(tear_down):
    @yetl_flow(
            project="test_project", 
            stage=StageType.audit_control, 
            config_path="./test/config"
    )
    def autoloader(table_mapping:TableMapping):
        return table_mapping
    

    result = autoloader(table="header_footer")
    tear_down()
    assert result.source.table == "customers"
    assert result.destination.table == "header_footer"



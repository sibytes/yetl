from dbxconfig import Config, Timeslice, StageType, Read, DeltaLake, yetl_flow, TableMapping
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


def test_configuration_load(tear_down):
    tear_down()
    pipeline = "auto_load_schema"
    config_path = "./test/config"
    project = "test_project"
    timeslice = Timeslice(day="*", month="*", year="*")
    config = Config(
        project=project, pipeline=pipeline, config_path=config_path
    )
    table_mapping = config.get_table_mapping(
        timeslice=timeslice, stage=StageType.raw, table="customers"
    )

    source: Read = table_mapping.source["customer_details_1"]
    destination: DeltaLake = table_mapping.destination
    config.set_checkpoint(source=source, destination=destination)

    assert source.table == "customer_details_1"
    assert destination.table == "customers"


def test_decorator_configuration_load(tear_down):
    @yetl_flow(
            project="test_project", 
            stage=StageType.raw, 
            config_path="./test/config"
    )
    def auto_load_schema(table_mapping:TableMapping):
        return table_mapping
    

    result = auto_load_schema(table="customers")
    tear_down()
    assert result.source["customer_details_1"].table == "customer_details_1"
    assert result.destination.table == "customers"



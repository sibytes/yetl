from dbxconfig import StageType, yetl_flow, TableMapping
import pytest
import os
import shutil

def tear_down():
    shutil.rmtree("./test/config/test_project/data", ignore_errors=True)
    shutil.rmtree("./metastore_db", ignore_errors=True)
    shutil.rmtree("./spark-warehouse", ignore_errors=True)
    try:
        os.remove("./derby.log")
    except Exception:
        pass


tear_down()


@yetl_flow(
        project="test_project", 
        stage=StageType.raw, 
        config_path="./test/config"
)
def auto_load_schema(table_mapping:TableMapping):

    destination = table_mapping.destination
    source = table_mapping.source
    assert source.table == "customer_details_1"
    assert destination.table == "customers"


auto_load_schema(table="customers")

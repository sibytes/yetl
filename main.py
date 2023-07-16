from yetl import *
import os
import shutil


# from yetl import __main__

def tear_down():
    shutil.rmtree("./test/config/test_project/data", ignore_errors=True)
    shutil.rmtree("./metastore_db", ignore_errors=True)
    shutil.rmtree("./spark-warehouse", ignore_errors=True)
    try:
        os.remove("./derby.log")
    except Exception:
        pass


tear_down()
pipeline = "autoloader"
config_path = "./test/config"
project = "test_project"
timeslice = Timeslice(day="*", month="*", year="*")
config = Config(
    project=project, 
    pipeline=pipeline, 
    config_path=config_path, 
    timeslice=timeslice,
)
table_mapping = config.get_table_mapping(
    stage=StageType.raw, table="customers", catalog_enabled=False
)

source: Read = table_mapping.source["customer_details_1"]
destination: DeltaLake = table_mapping.destination
config.set_checkpoint(source=source, destination=destination)


# t:Timeslice = Timeslice.parse_iso_date("*-*-")
# print(t.strftime("%Y%m%d"))
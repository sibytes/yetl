from yetl import StageType, yetl_flow, TableMapping, Timeslice
import pytest
import os
import shutil

# def tear_down():
#     shutil.rmtree("./test/config/test_project/data", ignore_errors=True)
#     shutil.rmtree("./metastore_db", ignore_errors=True)
#     shutil.rmtree("./spark-warehouse", ignore_errors=True)
#     try:
#         os.remove("./derby.log")
#     except Exception:
#         pass


# tear_down()


# @yetl_flow(
#         project="test_project", 
#         stage=StageType.raw, 
#         config_path="./test/config"
# )
# def auto_load_schema(table_mapping:TableMapping):

#     destination = table_mapping.destination
#     source = table_mapping.source["customer_details_1"]

#     assert source.table == "customer_details_1"
#     assert destination.table == "customers"
#     return table_mapping


# table_maping = auto_load_schema(table="customers")

# print(table_maping.destination)



t:Timeslice = Timeslice.parse_iso_date("*-*-")
print(t.strftime("%Y%m%d"))
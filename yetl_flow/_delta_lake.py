import json
from pyspark.sql import DataFrame


def table_exists(context, database: str, table: str):
    table_exists = (
        context.spark.sql(f"SHOW TABLES in {database};")
        .where(f"tableName='{table}' AND !isTemporary")
        .count()
        == 1
    )
    return table_exists


def create_table(context, database: str, table: str, path: str, sql: str = None):
    context.log.info(f"Creating table if not exists {database}.{table} at {path}")

    if not sql:
        sql = f"""
            CREATE TABLE IF NOT EXISTS `{database}`.`{table}` 
            USING DELTA LOCATION '{path}';"""

    context.spark.sql(sql)


def create_database(context, database: str):
    context.log.info(f"Creating database if not exists `{database}`")
    sql = f"CREATE DATABASE IF NOT EXISTS {database}"
    context.log.debug(sql)
    context.spark.sql(sql)


def get_audit(context, database_table: str):
    context.log.info(f"Auditing database table {database_table}")
    sql = f"DESCRIBE HISTORY {database_table}"
    context.log.debug(sql)
    audit = context.spark.sql(sql).first().asDict()
    return audit


def get_audits(context):
    audit = {}
    for database_table, destination in context.dataflow.destinations.items():
        audit[database_table] = get_audit(context, destination.database_table)

    results = json.dumps(audit, indent=4, sort_keys=True, default=str)

    context.log.info(results)
    return audit


def alter_table_drop_constraint(database: str, table: str, name: str):

    return f"ALTER TABLE `{database}`.`{table}` DROP CONSTRAINT {name};"


def alter_table_add_constraint(database: str, table: str, name: str, constraint: str):

    return f"ALTER TABLE `{database}`.`{table}` ADD CONSTRAINT {name} CHECK ({constraint});"


def alter_table_set_tblproperties(database: str, table: str, properties: str):

    return f"ALTER TABLE `{database}`.`{table}` SET TBLPROPERTIES ({properties});"


def get_table_properties(context, database: str, table: str):

    context.log.info(f"getting existing table properties for table {database}.{table}")
    df: DataFrame = context.spark.sql(
        f"SHOW TBLPROPERTIES `{database}`.`{table}`"
    ).collect()
    tbl_properties = {
        c.asDict()["key"]: c.asDict()["value"]
        for c in df
        if not c["key"].startswith("delta.constraints")
    }
    tbl_constraints = {
        c.asDict()["key"].split(".")[-1]: c.asDict()["value"]
        for c in df
        if c["key"].startswith("delta.constraints")
    }

    properties = {
        f"{database}.{table}": {
            "constraints": tbl_constraints,
            "properties": tbl_properties,
        }
    }
    msg = json.dumps(properties, indent=4, default=str)
    context.log.info(msg)
    return properties

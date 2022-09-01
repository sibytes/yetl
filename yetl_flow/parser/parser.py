import re


def sql_partitioned_by(sql: str):

    white_space = re.compile(r"\s+")
    sql = re.sub(white_space, "", sql).lower()

    partitioned_by = r"partitionedby\((.*)\)"
    partitions = re.search(partitioned_by, sql)
    if partitions:
        partitions_clause = partitions.group()
        partitions_clause = partitions_clause.replace("(", "")
        partitions_clause = partitions_clause.replace(")", "")
        partitions_clause = partitions_clause.replace("partitionedby", "")
        partitions_lst = partitions_clause.split(",")
        return partitions_lst
    else:
        return None

from yetl.model.context import BaseContext
from yetl.model.audit import Audit
from unittest import TestCase

config = {
    'datalake': '/Users/shaunryan/AzureDevOps/yetl/data', 
    'datalake_protocol': 'file:', 
    'spark': {
        'logging_level': 'ERROR', 
        'config': {
            'spark.master': 'local', 
            'spark.databricks.delta.allowArbitraryProperties.enabled': True, 
            'spark.jars.packages': 'io.delta:delta-core_2.12:2.1.1', 
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension', 
            'spark.sql.catalog.spark_catalog': 
            'org.apache.spark.sql.delta.catalog.DeltaCatalog', 
            'spark.databricks.delta.merge.repartitionBeforeWrite.enabled': True}
        }, 
        'pipeline_repo': 
        {
            'pipeline_file': 
            {
                'pipeline_root': './config/demo/pipelines', 
                'sql_root': './config/demo/sql'
            }
        }, 
        'spark_schema_repo': 
        {
            'spark_schema_file': {
                'spark_schema_root': './config/schema/spark'
            }
        }, 'deltalake_schema_repo': {
            'deltalake_sql_file': {
                'deltalake_schema_root': './config/schema/deltalake'
            }
        }, 'metadata_repo': {
            'metadata_file': {
                'metadata_root': './config/runs', 
                'metadata_dataset': 'dataset.json', 
                'metadata_index': 'index.json'
            }
        }
    }

def test_base_context():
    context = BaseContext(
        auditor = Audit(),
        project = "demo",
        name="demo",
    )

    TestCase().assertDictEqual(config, context.config)
    

    

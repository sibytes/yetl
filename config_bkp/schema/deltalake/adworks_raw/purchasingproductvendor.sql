CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`AverageLeadTime` long  ,
	`BusinessEntityID` long  ,
	`LastReceiptCost` double  ,
	`LastReceiptDate` string  ,
	`MaxOrderQty` long  ,
	`MinOrderQty` long  ,
	`ModifiedDate` string  ,
	`OnOrderQty` long  ,
	`ProductID` long  ,
	`StandardPrice` double  ,
	`UnitMeasureCode` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
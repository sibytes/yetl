CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`ActualCost` double  ,
	`ModifiedDate` string  ,
	`ProductID` long  ,
	`Quantity` long  ,
	`ReferenceOrderID` long  ,
	`ReferenceOrderLineID` long  ,
	`TransactionDate` string  ,
	`TransactionID` long  ,
	`TransactionType` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
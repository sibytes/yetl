CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`DueDate` string  ,
	`LineTotal` double  ,
	`ModifiedDate` string  ,
	`OrderQty` long  ,
	`ProductID` long  ,
	`PurchaseOrderDetailID` long  ,
	`PurchaseOrderID` long  ,
	`ReceivedQty` double  ,
	`RejectedQty` double  ,
	`StockedQty` double  ,
	`UnitPrice` double  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
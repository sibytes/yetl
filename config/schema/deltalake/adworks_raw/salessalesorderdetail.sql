CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`CarrierTrackingNumber` string  ,
	`LineTotal` double  ,
	`ModifiedDate` string  ,
	`OrderQty` long  ,
	`ProductID` long  ,
	`SalesOrderDetailID` long  ,
	`SalesOrderID` long  ,
	`SpecialOfferID` long  ,
	`UnitPrice` double  ,
	`UnitPriceDiscount` double  ,
	`rowguid` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
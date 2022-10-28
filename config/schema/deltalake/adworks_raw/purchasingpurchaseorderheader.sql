CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`EmployeeID` long  ,
	`Freight` double  ,
	`ModifiedDate` string  ,
	`OrderDate` string  ,
	`PurchaseOrderID` long  ,
	`RevisionNumber` long  ,
	`ShipDate` string  ,
	`ShipMethodID` long  ,
	`Status` long  ,
	`SubTotal` double  ,
	`TaxAmt` double  ,
	`TotalDue` double  ,
	`VendorID` long  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
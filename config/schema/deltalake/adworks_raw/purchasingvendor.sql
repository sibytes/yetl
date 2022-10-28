CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`AccountNumber` string  ,
	`ActiveFlag` boolean  ,
	`BusinessEntityID` long  ,
	`CreditRating` long  ,
	`ModifiedDate` string  ,
	`Name` string  ,
	`PreferredVendorStatus` boolean  ,
	`PurchasingWebServiceURL` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
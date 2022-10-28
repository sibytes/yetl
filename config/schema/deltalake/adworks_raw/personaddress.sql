CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`AddressID` long  ,
	`AddressLine1` string  ,
	`AddressLine2` string  ,
	`City` string  ,
	`ModifiedDate` string  ,
	`PostalCode` string  ,
	`SpatialLocation` string  ,
	`StateProvinceID` long  ,
	`rowguid` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`Bonus` double  ,
	`BusinessEntityID` long  ,
	`CommissionPct` double  ,
	`ModifiedDate` string  ,
	`SalesLastYear` double  ,
	`SalesQuota` double  ,
	`SalesYTD` double  ,
	`TerritoryID` long  ,
	`rowguid` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
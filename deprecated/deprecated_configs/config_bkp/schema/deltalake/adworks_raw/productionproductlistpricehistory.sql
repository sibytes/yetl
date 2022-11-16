CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`EndDate` string  ,
	`ListPrice` double  ,
	`ModifiedDate` string  ,
	`ProductID` long  ,
	`StartDate` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
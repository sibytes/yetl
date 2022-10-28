CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`ModifiedDate` string  ,
	`Name` string  ,
	`UnitMeasureCode` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
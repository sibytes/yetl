CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`DepartmentID` long  ,
	`GroupName` string  ,
	`ModifiedDate` string  ,
	`Name` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`id` integer  ,
	`allow_contact` boolean  ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`id` integer  ,
	`first_name` string  ,
	`last_name` string  ,
	`email` string  ,
	`gender` string  ,
	`job_title` string  ,
	`amount` double  ,
	`allow_contact` boolean  ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_filepath` string NOT NULL ,
	`_filename` string NOT NULL ,
	`_partition_key` integer  ,
	`_context_id` string NOT NULL ,
	`_dataflow_id` string NOT NULL ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
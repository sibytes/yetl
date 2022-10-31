CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`ChangeNumber` long  ,
	`Document` string  ,
	`DocumentLevel` long  ,
	`DocumentNode` string  ,
	`DocumentSummary` string  ,
	`FileExtension` string  ,
	`FileName` string  ,
	`FolderFlag` boolean  ,
	`ModifiedDate` string  ,
	`Owner` long  ,
	`Revision` string  ,
	`Status` long  ,
	`Title` string  ,
	`rowguid` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
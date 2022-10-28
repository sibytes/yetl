CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`LargePhoto` string  ,
	`LargePhotoFileName` string  ,
	`ModifiedDate` string  ,
	`ProductPhotoID` long  ,
	`ThumbNailPhoto` string  ,
	`ThumbnailPhotoFileName` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
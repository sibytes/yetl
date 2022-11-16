CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`Category` string  ,
	`Description` string  ,
	`DiscountPct` double  ,
	`EndDate` string  ,
	`MaxQty` long  ,
	`MinQty` long  ,
	`ModifiedDate` string  ,
	`SpecialOfferID` long  ,
	`StartDate` string  ,
	`Type` string  ,
	`rowguid` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
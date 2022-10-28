CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`DueDate` string  ,
	`EndDate` string  ,
	`ModifiedDate` string  ,
	`OrderQty` long  ,
	`ProductID` long  ,
	`ScrapReasonID` long  ,
	`ScrappedQty` long  ,
	`StartDate` string  ,
	`StockedQty` long  ,
	`WorkOrderID` long  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
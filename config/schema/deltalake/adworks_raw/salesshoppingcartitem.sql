CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`DateCreated` string  ,
	`ModifiedDate` string  ,
	`ProductID` long  ,
	`Quantity` long  ,
	`ShoppingCartID` string  ,
	`ShoppingCartItemID` long  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
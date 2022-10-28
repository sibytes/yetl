CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`AdditionalContactInfo` string  ,
	`BusinessEntityID` long  ,
	`Demographics` string  ,
	`EmailPromotion` long  ,
	`FirstName` string  ,
	`LastName` string  ,
	`MiddleName` string  ,
	`ModifiedDate` string  ,
	`NameStyle` boolean  ,
	`PersonType` string  ,
	`Suffix` string  ,
	`Title` string  ,
	`rowguid` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
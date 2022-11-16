CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`BirthDate` string  ,
	`BusinessEntityID` long  ,
	`CurrentFlag` boolean  ,
	`Gender` string  ,
	`HireDate` string  ,
	`JobTitle` string  ,
	`LoginID` string  ,
	`MaritalStatus` string  ,
	`ModifiedDate` string  ,
	`NationalIDNumber` string  ,
	`OrganizationLevel` long  ,
	`OrganizationNode` string  ,
	`SalariedFlag` boolean  ,
	`SickLeaveHours` long  ,
	`VacationHours` long  ,
	`rowguid` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
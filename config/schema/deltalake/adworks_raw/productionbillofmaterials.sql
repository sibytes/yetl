CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`BOMLevel` long  ,
	`BillOfMaterialsID` long  ,
	`ComponentID` long  ,
	`EndDate` string  ,
	`ModifiedDate` string  ,
	`PerAssemblyQty` double  ,
	`ProductAssemblyID` long  ,
	`StartDate` string  ,
	`UnitMeasureCode` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`Class` string  ,
	`Color` string  ,
	`DaysToManufacture` long  ,
	`DiscontinuedDate` string  ,
	`FinishedGoodsFlag` boolean  ,
	`ListPrice` double  ,
	`MakeFlag` boolean  ,
	`ModifiedDate` string  ,
	`Name` string  ,
	`ProductID` long  ,
	`ProductLine` string  ,
	`ProductModelID` long  ,
	`ProductNumber` string  ,
	`ProductSubcategoryID` long  ,
	`ReorderPoint` long  ,
	`SafetyStockLevel` long  ,
	`SellEndDate` string  ,
	`SellStartDate` string  ,
	`Size` string  ,
	`SizeUnitMeasureCode` string  ,
	`StandardCost` double  ,
	`Style` string  ,
	`Weight` double  ,
	`WeightUnitMeasureCode` string  ,
	`rowguid` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
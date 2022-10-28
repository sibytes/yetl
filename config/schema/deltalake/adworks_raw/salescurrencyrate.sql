CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`AverageRate` double  ,
	`CurrencyRateDate` string  ,
	`CurrencyRateID` long  ,
	`EndOfDayRate` double  ,
	`FromCurrencyCode` string  ,
	`ModifiedDate` string  ,
	`ToCurrencyCode` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
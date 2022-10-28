CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`AccountNumber` string  ,
	`BillToAddressID` long  ,
	`Comment` string  ,
	`CreditCardApprovalCode` string  ,
	`CreditCardID` long  ,
	`CurrencyRateID` long  ,
	`CustomerID` long  ,
	`DueDate` string  ,
	`Freight` double  ,
	`ModifiedDate` string  ,
	`OnlineOrderFlag` boolean  ,
	`OrderDate` string  ,
	`PurchaseOrderNumber` string  ,
	`RevisionNumber` long  ,
	`SalesOrderID` long  ,
	`SalesOrderNumber` string  ,
	`SalesPersonID` long  ,
	`ShipDate` string  ,
	`ShipMethodID` long  ,
	`ShipToAddressID` long  ,
	`Status` long  ,
	`SubTotal` double  ,
	`TaxAmt` double  ,
	`TerritoryID` long  ,
	`TotalDue` double  ,
	`rowguid` string  ,
	`_context_id` string NOT NULL ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_partition_key` integer  
)
USING DELTA LOCATION '{{ path }}'
PARTITIONED BY (`_partition_key`);
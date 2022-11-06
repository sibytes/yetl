CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`id` integer  ,
	`first_name` string  ,
	`last_name` string  ,
	`email` string  ,
	`gender` string  ,
	`job_title` string  ,
	`amount` double  ,
	`allow_contact` boolean  ,
	`_from_datetime` timestamp NOT NULL ,
	`_to_datetime` timestamp  ,
	`_current` boolean NOT NULL ,
	`_deleted` boolean NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
;
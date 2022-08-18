CREATE TABLE {{database_name}}.{{table_name}}
(
    id            integer not null,
    first_name    string not null,
    last_name     string,
    email         string,
    gender        string,
    job_title     string,
    amount        double,
    allow_contact boolean,
    _partition_key int,
    _correlation_id string
)
USING DELTA LOCATION '{{path}}'
PARTITIONED BY (_partition_key);
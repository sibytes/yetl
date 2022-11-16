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
    _context_id string,
    _timeslice timestamp
)
USING DELTA LOCATION '{{path}}'
PARTITIONED BY (_partition_key,allow_contact);
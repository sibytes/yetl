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
    _from_date     timestamp,
    _to_date       timestamp,
    _current       boolean,
    _deleted_date  timestamp,
    _partition_key int,
    _context_id string,
    _timeslice timestamp
)
USING DELTA LOCATION '{{path}}';
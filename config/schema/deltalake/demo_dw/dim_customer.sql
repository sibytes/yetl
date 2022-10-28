CREATE TABLE {{database_name}}.{{table_name}}
(
    id             integer not null,
    first_name     string not null,
    last_name      string,
    email          string,
    gender         string,
    job_title      string,
    amount         double,
    allow_contact  boolean,
    _from_datetime timestamp,
    _to_datetime   timestamp,
    _current       boolean,
    _deleted       boolean,
    _partition_key int,
    _context_id    string
)
USING DELTA LOCATION '{{path}}';
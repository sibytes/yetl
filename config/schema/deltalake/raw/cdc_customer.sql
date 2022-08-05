CREATE TABLE {{database_name}}.{{table_name}}
(
    extract_date  timestamp,
    load_flag     string,
    id            integer not null,
    first_name    string not null,
    last_name     string,
    email         string,
    gender        string,
    job_title     string,
    amount        double
)
USING DELTA LOCATION '{{path}}';

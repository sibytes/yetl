CREATE TABLE {{database_name}}.{{table_name}}
(
    extract_date  timestamp not null,
    load_flag     string not null,
    id            integer not null,
    first_name    string not null,
    last_name     string,
    email         string,
    gender        string,
    job_title     string,
    amount        double,
    from_date     date not null,
    to_date       date not null,
    active        boolean not null
)
USING DELTA LOCATION '{{path}}';

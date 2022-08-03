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
    from_date     datetime,
    to_date       datetime,
    current       boolean,
    deleted_date  datetime
)
USING DELTA LOCATION '{{path}}';
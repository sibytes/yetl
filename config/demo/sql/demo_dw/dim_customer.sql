SELECT
  id            ,
  first_name    ,
  last_name     ,
  email         ,
  gender        ,
  job_title     ,
  amount        ,
  allow_contact ,
  current_timestamp() as _from_datetime,
  to_timestamp('9999-12-31 23:59:59.999') as _to_datetime,
  true as _current,
  false as _deleted  
FROM demo_raw.customer
-- WHERE _TIMESLICE = {{timeslice}}


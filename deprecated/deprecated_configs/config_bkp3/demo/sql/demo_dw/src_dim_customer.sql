SELECT
  cd.id            ,
  cd.first_name    ,
  cd.last_name     ,
  cd.email         ,
  cd.gender        ,
  cd.job_title     ,
  cd.amount        ,
  cp.allow_contact ,
  cd._timeslice    ,
  current_timestamp() as _from_datetime,
  to_timestamp('9999-12-31 23:59:59.999') as _to_datetime,
  true as _current,
  false as _deleted  
FROM demo_raw.customer_details cd
JOIN demo_raw.customer_preferences cp on cd.id = cp.id
-- WHERE _TIMESLICE = {{timeslice}}


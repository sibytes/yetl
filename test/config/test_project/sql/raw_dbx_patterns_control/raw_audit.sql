  CREATE TABLE IF NOT EXISTS `raw_dbx_patterns_control`.`raw_audit`
  (
    total_count bigint,
    valid_count bigint,
    invalid_count bigint,
    invalid_ratio double,
    expected_row_count bigint,
    _process_id bigint,
    _load_date timestamp,
    file_name string,
    file_path string,
    file_size bigint,
    file_modification_time timestamp
  )
  USING DELTA
  LOCATION '{{location}}'
  TBLPROPERTIES (
    {{delta_properties}}
  )
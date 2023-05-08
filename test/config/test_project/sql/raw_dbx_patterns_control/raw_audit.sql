  CREATE TABLE IF NOT EXISTS `raw_dbx_patterns_control`.`raw_audit`
  (
    `database` string,
    `table` string,
    `file_name` string,
    total_count bigint,
    valid_count bigint,
    invalid_count bigint,
    invalid_ratio double,
    expected_row_count bigint,
    validation_thresholds struct<
      invalid_ratio:double,
      invalid_rows:bigint,
      max_rows:bigint,
      min_rows:bigint
    >,
    exception_thresholds struct<
      invalid_ratio:double,
      invalid_rows:bigint,
      max_rows:bigint,
      min_rows:bigint
    >,
    file_path string,
    file_size bigint,
    file_modification_time timestamp,
    _process_id bigint,
    _load_date timestamp
  )
  USING DELTA
  LOCATION '{{location}}'
  TBLPROPERTIES (
    {{delta_properties}}
  )

  

    SELECT 
    lower(object_name)   AS snapshot_table_name,
    '(?i)'||object_schema ||'.'||object_name || '/.*\.parquet' AS file_location,
     lower(object_name) || '_transient' AS transient_table_name,
        to_char(object_key_columns) AS key_column,
        to_char(object_columns) as all_column
FROM 
    pipeline_audit_dev.config.object_catalog 
WHERE 
    object_schema IN ( 'DBO')
    and object_database = 'SASREFERENCET'
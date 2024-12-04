{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

WITH dbo_reqdriver_transient_source_data AS (
    SELECT 
        -- Call the macro to get the dynamic list of columns
        {{ get_dynamic_columns('SASREFERENCE', 'DBO_REQDRIVER_REFINED') }},
        CURRENT_TIMESTAMP() AS CREATED_DATETIME,
        CURRENT_USER() AS CREATED_BY
    FROM 
        bronze_zone_dev.sasreference.dbo_reqdriver_transient
)

-- Final selection of the data
SELECT * 
FROM dbo_reqdriver_transient_source_data;

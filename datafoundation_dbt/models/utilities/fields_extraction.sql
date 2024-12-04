-- This DBT model extracts and flattens the JSON data to get the field names and types

{{ config(
    profile = 'snowflake',
    materialized = 'table'
)}}


WITH raw_data AS (
    SELECT 
        -- Assuming the table has a column `fields_metadata` of type JSON
        object_schema_script
    FROM 
        {{ source('sql', 'object_catalog') }}  -- Reference your source table
),

flattened_data AS (
    SELECT
        -- Flatten the JSON array using OPENJSON and extract field names and types
        field.value AS field_data
    FROM 
        raw_data
        CROSS APPLY OPENJSON(raw_data.object_schema_script) AS field  -- Flatten the JSON array
)

SELECT
    JSON_VALUE(field_data, '$.name') AS field_name,  -- Extract field name
    JSON_VALUE(field_data, '$.type') AS field_type   -- Extract field type
FROM 
    flattened_data

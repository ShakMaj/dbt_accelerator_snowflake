{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    unique_key = 'UNDLY_UNIT_COV_ID', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.undly_unit_cov_transient', false, '(?i)TDB2WPLQ.UNDLY_UNIT_COV/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.undly_unit_cov_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(UNDLY_UNIT_COV_ID)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH undly_unit_cov_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'UNDLY_UNIT_COV_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.undly_unit_cov_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.UNDLY_UNIT_COV_ID AS SOURCE_UNDLY_UNIT_COV_ID,  -- Source IDs
        t.UNDLY_UNIT_COV_ID AS TARGET_UNDLY_UNIT_COV_ID,  -- Target IDs
        CASE 
            WHEN t.UNDLY_UNIT_COV_ID IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM undly_unit_cov_source_data s
    LEFT JOIN {{ this }} t
    ON s.UNDLY_UNIT_COV_ID = t.UNDLY_UNIT_COV_ID
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM undly_unit_cov_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.UNDLY_UNIT_COV_ID = st.SOURCE_UNDLY_UNIT_COV_ID
    LEFT JOIN {{ this }} t
        ON s.UNDLY_UNIT_COV_ID = t.UNDLY_UNIT_COV_ID
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'UNDLY_UNIT_COV_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(UNDLY_UNIT_COV_ID), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

    -- Set CREATED_DATETIME only on inserts
    , CASE 
        WHEN FLAG = 'I' THEN CURRENT_TIMESTAMP() 
        ELSE AUDIT_CREATED_DATETIME  -- Keep the same value for updates
    END AS AUDIT_CREATED_DATETIME
    
    -- Set CREATED_BY only on inserts
    , CASE 
        WHEN FLAG = 'I' THEN CURRENT_USER()  
        ELSE AUDIT_CREATED_BY  -- Keep the same value for updates
    END AS AUDIT_CREATED_BY                          

    , CURRENT_TIMESTAMP() as AUDIT_MODIFIED_DATETIME
    , CURRENT_USER() as AUDIT_MODIFIED_BY

FROM joined_data

{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    transient = false,
    unique_key = 'CLCT_ID', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.wescom_pl.ref_clct_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.wescom_pl.ref_clct",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.wescom_pl.ref_clct AS SELECT * FROM bronze_zone_dev.wescom_pl.ref_clct_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.ref_clct_transient', true, '(?i)TDB2WPLQ.REF_CLCT/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.ref_clct_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(CLCT_ID)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_wescom_pl.ref_clct AS SELECT * FROM bronze_zone_dev.wescom_pl.ref_clct_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH ref_clct_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'REF_CLCT_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.ref_clct_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.CLCT_ID AS SOURCE_CLCT_ID,  -- Source IDs
        t.CLCT_ID AS TARGET_CLCT_ID,  -- Target IDs
        CASE 
            --WHEN t.CLCT_ID IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.CLCT_ID) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM ref_clct_source_data s
    LEFT JOIN {{ this }} t
    ON s.CLCT_ID = t.CLCT_ID
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM ref_clct_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.CLCT_ID = st.SOURCE_CLCT_ID
    LEFT JOIN {{ this }} t
        ON s.CLCT_ID = t.CLCT_ID
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'REF_CLCT_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(CLCT_ID,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

    -- Set CREATED_DATETIME only on inserts
    , CASE 
        WHEN UPSERT_FLAG = 'I' THEN CURRENT_TIMESTAMP() 
        ELSE AUDIT_CREATED_DATETIME  -- Keep the same value for updates
    END AS AUDIT_CREATED_DATETIME
    
    -- Set CREATED_BY only on inserts
    , CASE 
        WHEN UPSERT_FLAG = 'I' THEN CURRENT_USER()  
        ELSE AUDIT_CREATED_BY  -- Keep the same value for updates
    END AS AUDIT_CREATED_BY                          

    , CURRENT_TIMESTAMP() as AUDIT_MODIFIED_DATETIME
    , CURRENT_USER() as AUDIT_MODIFIED_BY

FROM joined_data

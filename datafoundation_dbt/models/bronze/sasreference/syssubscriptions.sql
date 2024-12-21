{{ config(
    schema = 'sasreference',
    materialized = 'table',
    transient = false,
    unique_key = 'ARTID, SRVID, DEST_DB, SRVNAME', 
    tags=["bronze", 'sasreference'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.sasreference.syssubscriptions_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.sasreference.syssubscriptions",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.sasreference.syssubscriptions AS SELECT * FROM bronze_zone_dev.sasreference.syssubscriptions_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.sasreference.sasreference_stage', 'bronze_zone_dev.sasreference.syssubscriptions_transient', true, '(?i)DBO.syssubscriptions/.*.parquet') }}",
         "UPDATE bronze_zone_dev.sasreference.syssubscriptions_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(ARTID,' | ',SRVID,' | ',DEST_DB,' | ',SRVNAME)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_sasreference.syssubscriptions AS SELECT * FROM bronze_zone_dev.sasreference.syssubscriptions_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH syssubscriptions_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('SASREFERENCE', 'SYSSUBSCRIPTIONS_TRANSIENT') }},
    FROM 
        bronze_zone_dev.sasreference.syssubscriptions_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.ARTID AS SOURCE_ARTID, s.SRVID AS SOURCE_SRVID, s.DEST_DB AS SOURCE_DEST_DB, s.SRVNAME AS SOURCE_SRVNAME,  -- Source IDs
        t.ARTID AS TARGET_ARTID, t.SRVID AS TARGET_SRVID, t.DEST_DB AS TARGET_DEST_DB, t.SRVNAME AS TARGET_SRVNAME,  -- Target IDs
        CASE 
            --WHEN t.ARTID OR t.SRVID OR t.DEST_DB OR t.SRVNAME IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.ARTID) OR TO_CHAR(t.SRVID) OR TO_CHAR(t.DEST_DB) OR TO_CHAR(t.SRVNAME) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM syssubscriptions_source_data s
    LEFT JOIN {{ this }} t
    ON s.ARTID = t.ARTID AND s.SRVID = t.SRVID AND s.DEST_DB = t.DEST_DB AND s.SRVNAME = t.SRVNAME
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM syssubscriptions_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.ARTID = st.SOURCE_ARTID AND s.SRVID = st.SOURCE_SRVID AND s.DEST_DB = st.SOURCE_DEST_DB AND s.SRVNAME = st.SOURCE_SRVNAME
    LEFT JOIN {{ this }} t
        ON s.ARTID = t.ARTID AND s.SRVID = t.SRVID AND s.DEST_DB = t.DEST_DB AND s.SRVNAME = t.SRVNAME
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('SASREFERENCE', 'SYSSUBSCRIPTIONS_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(ARTID, SRVID, DEST_DB, SRVNAME,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

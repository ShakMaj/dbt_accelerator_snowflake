{{ config(
    schema = 'sasreference',
    materialized = 'table',
    transient = false,
    unique_key = 'ID, ORIGINATOR, ORIGINATOR_DB, ORIGINATOR_PUBLICATION_ID, ORIGINATOR_DB_VERSION, ORIGINATOR_LSN', 
    tags=["bronze", 'sasreference'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.sasreference.mspeer_lsns_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.sasreference.mspeer_lsns",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.sasreference.mspeer_lsns AS SELECT * FROM bronze_zone_dev.sasreference.mspeer_lsns_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.sasreference.sasreference_stage', 'bronze_zone_dev.sasreference.mspeer_lsns_transient', true, '(?i)DBO.MSpeer_lsns/.*.parquet') }}",
         "UPDATE bronze_zone_dev.sasreference.mspeer_lsns_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(ID,' | ',ORIGINATOR,' | ',ORIGINATOR_DB,' | ',ORIGINATOR_PUBLICATION_ID,' | ',ORIGINATOR_DB_VERSION,' | ',ORIGINATOR_LSN)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_sasreference.mspeer_lsns AS SELECT * FROM bronze_zone_dev.sasreference.mspeer_lsns_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH mspeer_lsns_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('SASREFERENCE', 'MSPEER_LSNS_TRANSIENT') }},
    FROM 
        bronze_zone_dev.sasreference.mspeer_lsns_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.ID AS SOURCE_ID, s.ORIGINATOR AS SOURCE_ORIGINATOR, s.ORIGINATOR_DB AS SOURCE_ORIGINATOR_DB, s.ORIGINATOR_PUBLICATION_ID AS SOURCE_ORIGINATOR_PUBLICATION_ID, s.ORIGINATOR_DB_VERSION AS SOURCE_ORIGINATOR_DB_VERSION, s.ORIGINATOR_LSN AS SOURCE_ORIGINATOR_LSN,  -- Source IDs
        t.ID AS TARGET_ID, t.ORIGINATOR AS TARGET_ORIGINATOR, t.ORIGINATOR_DB AS TARGET_ORIGINATOR_DB, t.ORIGINATOR_PUBLICATION_ID AS TARGET_ORIGINATOR_PUBLICATION_ID, t.ORIGINATOR_DB_VERSION AS TARGET_ORIGINATOR_DB_VERSION, t.ORIGINATOR_LSN AS TARGET_ORIGINATOR_LSN,  -- Target IDs
        CASE 
            --WHEN t.ID OR t.ORIGINATOR OR t.ORIGINATOR_DB OR t.ORIGINATOR_PUBLICATION_ID OR t.ORIGINATOR_DB_VERSION OR t.ORIGINATOR_LSN IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.ID) OR TO_CHAR(t.ORIGINATOR) OR TO_CHAR(t.ORIGINATOR_DB) OR TO_CHAR(t.ORIGINATOR_PUBLICATION_ID) OR TO_CHAR(t.ORIGINATOR_DB_VERSION) OR TO_CHAR(t.ORIGINATOR_LSN) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM mspeer_lsns_source_data s
    LEFT JOIN {{ this }} t
    ON s.ID = t.ID AND s.ORIGINATOR = t.ORIGINATOR AND s.ORIGINATOR_DB = t.ORIGINATOR_DB AND s.ORIGINATOR_PUBLICATION_ID = t.ORIGINATOR_PUBLICATION_ID AND s.ORIGINATOR_DB_VERSION = t.ORIGINATOR_DB_VERSION AND s.ORIGINATOR_LSN = t.ORIGINATOR_LSN
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM mspeer_lsns_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.ID = st.SOURCE_ID AND s.ORIGINATOR = st.SOURCE_ORIGINATOR AND s.ORIGINATOR_DB = st.SOURCE_ORIGINATOR_DB AND s.ORIGINATOR_PUBLICATION_ID = st.SOURCE_ORIGINATOR_PUBLICATION_ID AND s.ORIGINATOR_DB_VERSION = st.SOURCE_ORIGINATOR_DB_VERSION AND s.ORIGINATOR_LSN = st.SOURCE_ORIGINATOR_LSN
    LEFT JOIN {{ this }} t
        ON s.ID = t.ID AND s.ORIGINATOR = t.ORIGINATOR AND s.ORIGINATOR_DB = t.ORIGINATOR_DB AND s.ORIGINATOR_PUBLICATION_ID = t.ORIGINATOR_PUBLICATION_ID AND s.ORIGINATOR_DB_VERSION = t.ORIGINATOR_DB_VERSION AND s.ORIGINATOR_LSN = t.ORIGINATOR_LSN
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('SASREFERENCE', 'MSPEER_LSNS_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(ID, ORIGINATOR, ORIGINATOR_DB, ORIGINATOR_PUBLICATION_ID, ORIGINATOR_DB_VERSION, ORIGINATOR_LSN,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

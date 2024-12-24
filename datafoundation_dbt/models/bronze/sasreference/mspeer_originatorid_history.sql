{{ config(
    schema = 'sasreference',
    materialized = 'table',
    unique_key = 'ORIGINATOR_PUBLICATION, ORIGINATOR_ID, ORIGINATOR_NODE, ORIGINATOR_DB, ORIGINATOR_DB_VERSION', 
    tags=["bronze", 'sasreference'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.sasreference.sasreference_stage', 'bronze_zone_dev.sasreference.mspeer_originatorid_history_transient', false, '(?i)DBO.MSpeer_originatorid_history/.*.parquet') }}",
         "UPDATE bronze_zone_dev.sasreference.mspeer_originatorid_history_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(ORIGINATOR_PUBLICATION,' | ',ORIGINATOR_ID,' | ',ORIGINATOR_NODE,' | ',ORIGINATOR_DB,' | ',ORIGINATOR_DB_VERSION)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH mspeer_originatorid_history_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('SASREFERENCE', 'MSPEER_ORIGINATORID_HISTORY_TRANSIENT') }},
    FROM 
        bronze_zone_dev.sasreference.mspeer_originatorid_history_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.ORIGINATOR_PUBLICATION AS SOURCE_ORIGINATOR_PUBLICATION, s.ORIGINATOR_ID AS SOURCE_ORIGINATOR_ID, s.ORIGINATOR_NODE AS SOURCE_ORIGINATOR_NODE, s.ORIGINATOR_DB AS SOURCE_ORIGINATOR_DB, s.ORIGINATOR_DB_VERSION AS SOURCE_ORIGINATOR_DB_VERSION,  -- Source IDs
        t.ORIGINATOR_PUBLICATION AS TARGET_ORIGINATOR_PUBLICATION, t.ORIGINATOR_ID AS TARGET_ORIGINATOR_ID, t.ORIGINATOR_NODE AS TARGET_ORIGINATOR_NODE, t.ORIGINATOR_DB AS TARGET_ORIGINATOR_DB, t.ORIGINATOR_DB_VERSION AS TARGET_ORIGINATOR_DB_VERSION,  -- Target IDs
        CASE 
            WHEN t.ORIGINATOR_PUBLICATION OR t.ORIGINATOR_ID OR t.ORIGINATOR_NODE OR t.ORIGINATOR_DB OR t.ORIGINATOR_DB_VERSION IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM mspeer_originatorid_history_source_data s
    LEFT JOIN {{ this }} t
    ON s.ORIGINATOR_PUBLICATION = t.ORIGINATOR_PUBLICATION AND s.ORIGINATOR_ID = t.ORIGINATOR_ID AND s.ORIGINATOR_NODE = t.ORIGINATOR_NODE AND s.ORIGINATOR_DB = t.ORIGINATOR_DB AND s.ORIGINATOR_DB_VERSION = t.ORIGINATOR_DB_VERSION
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM mspeer_originatorid_history_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.ORIGINATOR_PUBLICATION = st.SOURCE_ORIGINATOR_PUBLICATION AND s.ORIGINATOR_ID = st.SOURCE_ORIGINATOR_ID AND s.ORIGINATOR_NODE = st.SOURCE_ORIGINATOR_NODE AND s.ORIGINATOR_DB = st.SOURCE_ORIGINATOR_DB AND s.ORIGINATOR_DB_VERSION = st.SOURCE_ORIGINATOR_DB_VERSION
    LEFT JOIN {{ this }} t
        ON s.ORIGINATOR_PUBLICATION = t.ORIGINATOR_PUBLICATION AND s.ORIGINATOR_ID = t.ORIGINATOR_ID AND s.ORIGINATOR_NODE = t.ORIGINATOR_NODE AND s.ORIGINATOR_DB = t.ORIGINATOR_DB AND s.ORIGINATOR_DB_VERSION = t.ORIGINATOR_DB_VERSION
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('SASREFERENCE', 'MSPEER_ORIGINATORID_HISTORY_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(ORIGINATOR_PUBLICATION, ORIGINATOR_ID, ORIGINATOR_NODE, ORIGINATOR_DB, ORIGINATOR_DB_VERSION), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

{{ config(
    schema = 'sasreference',
    materialized = 'table',
    unique_key = 'REQUEST_ID, PEER_NODE, PEER_DB', 
    tags=["bronze", 'sasreference'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.sasreference.sasreference_stage', 'bronze_zone_dev.sasreference.mspeer_conflictdetectionconfigresponse_transient', false, '(?i)DBO.MSpeer_conflictdetectionconfigresponse/.*.parquet') }}",
         "UPDATE bronze_zone_dev.sasreference.mspeer_conflictdetectionconfigresponse_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(REQUEST_ID,' | ',PEER_NODE,' | ',PEER_DB)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH mspeer_conflictdetectionconfigresponse_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('SASREFERENCE', 'MSPEER_CONFLICTDETECTIONCONFIGRESPONSE_TRANSIENT') }},
    FROM 
        bronze_zone_dev.sasreference.mspeer_conflictdetectionconfigresponse_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.REQUEST_ID AS SOURCE_REQUEST_ID, s.PEER_NODE AS SOURCE_PEER_NODE, s.PEER_DB AS SOURCE_PEER_DB,  -- Source IDs
        t.REQUEST_ID AS TARGET_REQUEST_ID, t.PEER_NODE AS TARGET_PEER_NODE, t.PEER_DB AS TARGET_PEER_DB,  -- Target IDs
        CASE 
            WHEN t.REQUEST_ID OR t.PEER_NODE OR t.PEER_DB IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM mspeer_conflictdetectionconfigresponse_source_data s
    LEFT JOIN {{ this }} t
    ON s.REQUEST_ID = t.REQUEST_ID AND s.PEER_NODE = t.PEER_NODE AND s.PEER_DB = t.PEER_DB
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM mspeer_conflictdetectionconfigresponse_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.REQUEST_ID = st.SOURCE_REQUEST_ID AND s.PEER_NODE = st.SOURCE_PEER_NODE AND s.PEER_DB = st.SOURCE_PEER_DB
    LEFT JOIN {{ this }} t
        ON s.REQUEST_ID = t.REQUEST_ID AND s.PEER_NODE = t.PEER_NODE AND s.PEER_DB = t.PEER_DB
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('SASREFERENCE', 'MSPEER_CONFLICTDETECTIONCONFIGRESPONSE_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(REQUEST_ID, PEER_NODE, PEER_DB), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

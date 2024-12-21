{{ config(
    schema = 'sasreference',
    materialized = 'table',
    transient = false,
    unique_key = 'ID, REQUESTID, PGID, POLICYNUM, DRIVERID, POLICYSOURCETYPE', 
    tags=["bronze", 'sasreference'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.sasreference.reqdriver_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.sasreference.reqdriver",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.sasreference.reqdriver AS SELECT * FROM bronze_zone_dev.sasreference.reqdriver_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.sasreference.sasreference_stage', 'bronze_zone_dev.sasreference.reqdriver_transient', true, '(?i)DBO.ReqDriver/.*.parquet') }}",
         "UPDATE bronze_zone_dev.sasreference.reqdriver_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(ID,' | ',REQUESTID,' | ',PGID,' | ',POLICYNUM,' | ',DRIVERID,' | ',POLICYSOURCETYPE)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_sasreference.reqdriver AS SELECT * FROM bronze_zone_dev.sasreference.reqdriver_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH reqdriver_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('SASREFERENCE', 'REQDRIVER_TRANSIENT') }},
    FROM 
        bronze_zone_dev.sasreference.reqdriver_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.ID AS SOURCE_ID, s.REQUESTID AS SOURCE_REQUESTID, s.PGID AS SOURCE_PGID, s.POLICYNUM AS SOURCE_POLICYNUM, s.DRIVERID AS SOURCE_DRIVERID, s.POLICYSOURCETYPE AS SOURCE_POLICYSOURCETYPE,  -- Source IDs
        t.ID AS TARGET_ID, t.REQUESTID AS TARGET_REQUESTID, t.PGID AS TARGET_PGID, t.POLICYNUM AS TARGET_POLICYNUM, t.DRIVERID AS TARGET_DRIVERID, t.POLICYSOURCETYPE AS TARGET_POLICYSOURCETYPE,  -- Target IDs
        CASE 
            --WHEN t.ID OR t.REQUESTID OR t.PGID OR t.POLICYNUM OR t.DRIVERID OR t.POLICYSOURCETYPE IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.ID) OR TO_CHAR(t.REQUESTID) OR TO_CHAR(t.PGID) OR TO_CHAR(t.POLICYNUM) OR TO_CHAR(t.DRIVERID) OR TO_CHAR(t.POLICYSOURCETYPE) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM reqdriver_source_data s
    LEFT JOIN {{ this }} t
    ON s.ID = t.ID AND s.REQUESTID = t.REQUESTID AND s.PGID = t.PGID AND s.POLICYNUM = t.POLICYNUM AND s.DRIVERID = t.DRIVERID AND s.POLICYSOURCETYPE = t.POLICYSOURCETYPE
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM reqdriver_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.ID = st.SOURCE_ID AND s.REQUESTID = st.SOURCE_REQUESTID AND s.PGID = st.SOURCE_PGID AND s.POLICYNUM = st.SOURCE_POLICYNUM AND s.DRIVERID = st.SOURCE_DRIVERID AND s.POLICYSOURCETYPE = st.SOURCE_POLICYSOURCETYPE
    LEFT JOIN {{ this }} t
        ON s.ID = t.ID AND s.REQUESTID = t.REQUESTID AND s.PGID = t.PGID AND s.POLICYNUM = t.POLICYNUM AND s.DRIVERID = t.DRIVERID AND s.POLICYSOURCETYPE = t.POLICYSOURCETYPE
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('SASREFERENCE', 'REQDRIVER_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(ID, REQUESTID, PGID, POLICYNUM, DRIVERID, POLICYSOURCETYPE,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

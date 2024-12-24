{{ config(
    schema = 'sasreference',
    materialized = 'table',
    unique_key = 'ID, REQUESTID, PGID, LINEOFBUSINESS, VARIABLE, VALUETYPE', 
    tags=["bronze", 'sasreference'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.sasreference.sasreference_stage', 'bronze_zone_dev.sasreference.pruwanalyticaloutput_transient', false, '(?i)DBO.PruwAnalyticalOutput/.*.parquet') }}",
         "UPDATE bronze_zone_dev.sasreference.pruwanalyticaloutput_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(ID,' | ',REQUESTID,' | ',PGID,' | ',LINEOFBUSINESS,' | ',VARIABLE,' | ',VALUETYPE)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH pruwanalyticaloutput_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('SASREFERENCE', 'PRUWANALYTICALOUTPUT_TRANSIENT') }},
    FROM 
        bronze_zone_dev.sasreference.pruwanalyticaloutput_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.ID AS SOURCE_ID, s.REQUESTID AS SOURCE_REQUESTID, s.PGID AS SOURCE_PGID, s.LINEOFBUSINESS AS SOURCE_LINEOFBUSINESS, s.VARIABLE AS SOURCE_VARIABLE, s.VALUETYPE AS SOURCE_VALUETYPE,  -- Source IDs
        t.ID AS TARGET_ID, t.REQUESTID AS TARGET_REQUESTID, t.PGID AS TARGET_PGID, t.LINEOFBUSINESS AS TARGET_LINEOFBUSINESS, t.VARIABLE AS TARGET_VARIABLE, t.VALUETYPE AS TARGET_VALUETYPE,  -- Target IDs
        CASE 
            WHEN t.ID OR t.REQUESTID OR t.PGID OR t.LINEOFBUSINESS OR t.VARIABLE OR t.VALUETYPE IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM pruwanalyticaloutput_source_data s
    LEFT JOIN {{ this }} t
    ON s.ID = t.ID AND s.REQUESTID = t.REQUESTID AND s.PGID = t.PGID AND s.LINEOFBUSINESS = t.LINEOFBUSINESS AND s.VARIABLE = t.VARIABLE AND s.VALUETYPE = t.VALUETYPE
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM pruwanalyticaloutput_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.ID = st.SOURCE_ID AND s.REQUESTID = st.SOURCE_REQUESTID AND s.PGID = st.SOURCE_PGID AND s.LINEOFBUSINESS = st.SOURCE_LINEOFBUSINESS AND s.VARIABLE = st.SOURCE_VARIABLE AND s.VALUETYPE = st.SOURCE_VALUETYPE
    LEFT JOIN {{ this }} t
        ON s.ID = t.ID AND s.REQUESTID = t.REQUESTID AND s.PGID = t.PGID AND s.LINEOFBUSINESS = t.LINEOFBUSINESS AND s.VARIABLE = t.VARIABLE AND s.VALUETYPE = t.VALUETYPE
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('SASREFERENCE', 'PRUWANALYTICALOUTPUT_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(ID, REQUESTID, PGID, LINEOFBUSINESS, VARIABLE, VALUETYPE), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

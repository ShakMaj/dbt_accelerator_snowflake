{{ config(
    schema = 'sasreference',
    materialized = 'table',
    unique_key = 'LATCHTASKNAME, LATCHMACHINEGUID, LATCHKEY', 
    tags=["bronze", 'sasreference'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.sasreference.sasreference_stage', 'bronze_zone_dev.sasreference.attrep_truncation_safeguard_transient', false, '(?i)DBO.attrep_truncation_safeguard/.*.parquet') }}",
         "UPDATE bronze_zone_dev.sasreference.attrep_truncation_safeguard_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(LATCHTASKNAME,' | ',LATCHMACHINEGUID,' | ',LATCHKEY)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH attrep_truncation_safeguard_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('SASREFERENCE', 'ATTREP_TRUNCATION_SAFEGUARD_TRANSIENT') }},
    FROM 
        bronze_zone_dev.sasreference.attrep_truncation_safeguard_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.LATCHTASKNAME AS SOURCE_LATCHTASKNAME, s.LATCHMACHINEGUID AS SOURCE_LATCHMACHINEGUID, s.LATCHKEY AS SOURCE_LATCHKEY,  -- Source IDs
        t.LATCHTASKNAME AS TARGET_LATCHTASKNAME, t.LATCHMACHINEGUID AS TARGET_LATCHMACHINEGUID, t.LATCHKEY AS TARGET_LATCHKEY,  -- Target IDs
        CASE 
            WHEN t.LATCHTASKNAME OR t.LATCHMACHINEGUID OR t.LATCHKEY IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM attrep_truncation_safeguard_source_data s
    LEFT JOIN {{ this }} t
    ON s.LATCHTASKNAME = t.LATCHTASKNAME AND s.LATCHMACHINEGUID = t.LATCHMACHINEGUID AND s.LATCHKEY = t.LATCHKEY
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM attrep_truncation_safeguard_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.LATCHTASKNAME = st.SOURCE_LATCHTASKNAME AND s.LATCHMACHINEGUID = st.SOURCE_LATCHMACHINEGUID AND s.LATCHKEY = st.SOURCE_LATCHKEY
    LEFT JOIN {{ this }} t
        ON s.LATCHTASKNAME = t.LATCHTASKNAME AND s.LATCHMACHINEGUID = t.LATCHMACHINEGUID AND s.LATCHKEY = t.LATCHKEY
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('SASREFERENCE', 'ATTREP_TRUNCATION_SAFEGUARD_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(LATCHTASKNAME, LATCHMACHINEGUID, LATCHKEY), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

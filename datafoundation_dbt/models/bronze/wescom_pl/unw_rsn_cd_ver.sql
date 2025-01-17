{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    unique_key = 'UNW_RSN_CD_VER_ID, UNW_RSN_CD', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.unw_rsn_cd_ver_transient', false, '(?i)TDB2WPLQ.UNW_RSN_CD_VER/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.unw_rsn_cd_ver_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(UNW_RSN_CD_VER_ID,' | ',UNW_RSN_CD)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH unw_rsn_cd_ver_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'UNW_RSN_CD_VER_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.unw_rsn_cd_ver_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.UNW_RSN_CD_VER_ID AS SOURCE_UNW_RSN_CD_VER_ID, s.UNW_RSN_CD AS SOURCE_UNW_RSN_CD,  -- Source IDs
        t.UNW_RSN_CD_VER_ID AS TARGET_UNW_RSN_CD_VER_ID, t.UNW_RSN_CD AS TARGET_UNW_RSN_CD,  -- Target IDs
        CASE 
            WHEN t.UNW_RSN_CD_VER_ID OR t.UNW_RSN_CD IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM unw_rsn_cd_ver_source_data s
    LEFT JOIN {{ this }} t
    ON s.UNW_RSN_CD_VER_ID = t.UNW_RSN_CD_VER_ID AND s.UNW_RSN_CD = t.UNW_RSN_CD
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM unw_rsn_cd_ver_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.UNW_RSN_CD_VER_ID = st.SOURCE_UNW_RSN_CD_VER_ID AND s.UNW_RSN_CD = st.SOURCE_UNW_RSN_CD
    LEFT JOIN {{ this }} t
        ON s.UNW_RSN_CD_VER_ID = t.UNW_RSN_CD_VER_ID AND s.UNW_RSN_CD = t.UNW_RSN_CD
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'UNW_RSN_CD_VER_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(UNW_RSN_CD_VER_ID, UNW_RSN_CD), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

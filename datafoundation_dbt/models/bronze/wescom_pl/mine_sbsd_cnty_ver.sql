{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    unique_key = 'MINE_SBSDN_VER_ID, FIPS_CNTY_NR', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.mine_sbsd_cnty_ver_transient', false, '(?i)TDB2WPLQ.MINE_SBSD_CNTY_VER/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.mine_sbsd_cnty_ver_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(MINE_SBSDN_VER_ID,' | ',FIPS_CNTY_NR)) WHERE HASH_KEY_COLUMNS IS NULL"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH mine_sbsd_cnty_ver_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'AUDIT_CREATED_BY' and 'AUDIT_CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'MINE_SBSD_CNTY_VER_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.mine_sbsd_cnty_ver_transient s

        {% if is_incremental() %}
             WHERE s.INGESTDATE > (SELECT COALESCE(MAX(MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.MINE_SBSDN_VER_ID AS SOURCE_MINE_SBSDN_VER_ID, s.FIPS_CNTY_NR AS SOURCE_FIPS_CNTY_NR,  -- Source IDs
        t.MINE_SBSDN_VER_ID AS TARGET_MINE_SBSDN_VER_ID, t.FIPS_CNTY_NR AS TARGET_FIPS_CNTY_NR,  -- Target IDs
        CASE 
            WHEN t.MINE_SBSDN_VER_ID OR t.FIPS_CNTY_NR IS NULL THEN 'I'  -- Insert if no matching target ID
            ELSE 'U'  -- Update if target ID exists
        END AS FLAG
    FROM mine_sbsd_cnty_ver_source_data s
    LEFT JOIN {{ this }} t
    ON s.MINE_SBSDN_VER_ID = t.MINE_SBSDN_VER_ID AND s.FIPS_CNTY_NR = t.FIPS_CNTY_NR
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.FLAG AS FLAG
    FROM mine_sbsd_cnty_ver_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.MINE_SBSDN_VER_ID = st.SOURCE_MINE_SBSDN_VER_ID AND s.FIPS_CNTY_NR = st.SOURCE_FIPS_CNTY_NR
    LEFT JOIN {{ this }} t
        ON s.MINE_SBSDN_VER_ID = t.MINE_SBSDN_VER_ID AND s.FIPS_CNTY_NR = t.FIPS_CNTY_NR
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'MINE_SBSD_CNTY_VER_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , FLAG
    , SHA2(CONCAT(MINE_SBSDN_VER_ID, FIPS_CNTY_NR), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

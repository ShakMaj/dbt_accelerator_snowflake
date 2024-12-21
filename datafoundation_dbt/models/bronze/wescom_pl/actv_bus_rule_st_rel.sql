{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    transient = false,
    unique_key = 'MSG_CD, SEQ_NR, ST_CD, XCLD_IND', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.wescom_pl.actv_bus_rule_st_rel_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.wescom_pl.actv_bus_rule_st_rel",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.wescom_pl.actv_bus_rule_st_rel AS SELECT * FROM bronze_zone_dev.wescom_pl.actv_bus_rule_st_rel_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.actv_bus_rule_st_rel_transient', true, '(?i)TDB2WPLQ.ACTV_BUS_RULE_ST_REL/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.actv_bus_rule_st_rel_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(MSG_CD,' | ',SEQ_NR,' | ',ST_CD,' | ',XCLD_IND)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_wescom_pl.actv_bus_rule_st_rel AS SELECT * FROM bronze_zone_dev.wescom_pl.actv_bus_rule_st_rel_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH actv_bus_rule_st_rel_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'ACTV_BUS_RULE_ST_REL_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.actv_bus_rule_st_rel_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.MSG_CD AS SOURCE_MSG_CD, s.SEQ_NR AS SOURCE_SEQ_NR, s.ST_CD AS SOURCE_ST_CD, s.XCLD_IND AS SOURCE_XCLD_IND,  -- Source IDs
        t.MSG_CD AS TARGET_MSG_CD, t.SEQ_NR AS TARGET_SEQ_NR, t.ST_CD AS TARGET_ST_CD, t.XCLD_IND AS TARGET_XCLD_IND,  -- Target IDs
        CASE 
            --WHEN t.MSG_CD OR t.SEQ_NR OR t.ST_CD OR t.XCLD_IND IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.MSG_CD) OR TO_CHAR(t.SEQ_NR) OR TO_CHAR(t.ST_CD) OR TO_CHAR(t.XCLD_IND) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM actv_bus_rule_st_rel_source_data s
    LEFT JOIN {{ this }} t
    ON s.MSG_CD = t.MSG_CD AND s.SEQ_NR = t.SEQ_NR AND s.ST_CD = t.ST_CD AND s.XCLD_IND = t.XCLD_IND
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM actv_bus_rule_st_rel_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.MSG_CD = st.SOURCE_MSG_CD AND s.SEQ_NR = st.SOURCE_SEQ_NR AND s.ST_CD = st.SOURCE_ST_CD AND s.XCLD_IND = st.SOURCE_XCLD_IND
    LEFT JOIN {{ this }} t
        ON s.MSG_CD = t.MSG_CD AND s.SEQ_NR = t.SEQ_NR AND s.ST_CD = t.ST_CD AND s.XCLD_IND = t.XCLD_IND
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'ACTV_BUS_RULE_ST_REL_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(MSG_CD, SEQ_NR, ST_CD, XCLD_IND,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

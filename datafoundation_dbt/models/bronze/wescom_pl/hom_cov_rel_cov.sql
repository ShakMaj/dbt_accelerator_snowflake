{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    transient = false,
    unique_key = 'HOM_COV_ID, REL_COV_CD', 
    tags=["bronze", 'wescom_pl'],
    incremental_strategy = 'merge',
    pre_hook= [
        "delete from bronze_zone_dev.wescom_pl.hom_cov_rel_cov_transient",
        "DROP TABLE IF EXISTS bronze_zone_dev.wescom_pl.hom_cov_rel_cov",
        "CREATE TABLE IF NOT EXISTS bronze_zone_dev.wescom_pl.hom_cov_rel_cov AS SELECT * FROM bronze_zone_dev.wescom_pl.hom_cov_rel_cov_transient WHERE 1 = 0",
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.hom_cov_rel_cov_transient', true, '(?i)TDB2WPLQ.HOM_COV_REL_COV/.*.parquet') }}",
         "UPDATE bronze_zone_dev.wescom_pl.hom_cov_rel_cov_transient SET AUDIT_CREATED_DATETIME = CURRENT_TIMESTAMP(), AUDIT_CREATED_BY = CURRENT_USER(), HASH_KEY_COLUMNS = SHA2(CONCAT(HOM_COV_ID,' | ',REL_COV_CD)) WHERE HASH_KEY_COLUMNS IS NULL"
    ],
    post_hook= [
        "CREATE OR REPLACE VIEW publish_zone_dev.BRONZE_wescom_pl.hom_cov_rel_cov AS SELECT * FROM bronze_zone_dev.wescom_pl.hom_cov_rel_cov_transient"
    ]
) }}

-- Step 1: Source Data with FLAG Calculation
WITH hom_cov_rel_cov_source_data AS (
    SELECT 
        -- Dynamically get all columns from the source table except 'CREATED_BY' and 'CREATED_DATETIME'
        {{ get_dynamic_columns('WESCOM_PL', 'HOM_COV_REL_COV_TRANSIENT') }},
    FROM 
        bronze_zone_dev.wescom_pl.hom_cov_rel_cov_transient s

        {% if is_incremental() %}
             WHERE s.AUDIT_INGEST_DATETIME > (SELECT COALESCE(MAX(AUDIT_MODIFIED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})  -- Filter only modified records
        {% endif %}
),

-- Step 2: Source IDs from source and target to calculate FLAG
source_and_target_ids AS (
    SELECT 
        s.HOM_COV_ID AS SOURCE_HOM_COV_ID, s.REL_COV_CD AS SOURCE_REL_COV_CD,  -- Source IDs
        t.HOM_COV_ID AS TARGET_HOM_COV_ID, t.REL_COV_CD AS TARGET_REL_COV_CD,  -- Target IDs
        CASE 
            --WHEN t.HOM_COV_ID OR t.REL_COV_CD IS NULL THEN 'I'  -- Insert if no matching target ID
            WHEN TO_CHAR(t.HOM_COV_ID) OR TO_CHAR(t.REL_COV_CD) IS NULL THEN 'I'
            ELSE 'U'  -- Update if target ID exists
        END AS UPSERT_FLAG
    FROM hom_cov_rel_cov_source_data s
    LEFT JOIN {{ this }} t
    ON s.HOM_COV_ID = t.HOM_COV_ID AND s.REL_COV_CD = t.REL_COV_CD
),

-- Step 3: Join the FLAG Calculation with the Target Table
joined_data AS (
    SELECT 
        s.*,
        t.AUDIT_CREATED_BY AS AUDIT_CREATED_BY,
        t.AUDIT_CREATED_DATETIME AS AUDIT_CREATED_DATETIME,
        st.UPSERT_FLAG AS UPSERT_FLAG
    FROM hom_cov_rel_cov_source_data s
    LEFT JOIN source_and_target_ids st
        ON s.HOM_COV_ID = st.SOURCE_HOM_COV_ID AND s.REL_COV_CD = st.SOURCE_REL_COV_CD
    LEFT JOIN {{ this }} t
        ON s.HOM_COV_ID = t.HOM_COV_ID AND s.REL_COV_CD = t.REL_COV_CD
)


-- Step 4: Final Data Selection with Transformations
SELECT 
    -- Dynamically select all columns except 'CREATED_BY' and 'CREATED_DATETIME'
    {{ get_dynamic_columns('WESCOM_PL', 'HOM_COV_REL_COV_TRANSIENT') }}
    
    -- Include the FLAG and calculate the HASH_KEY
    , UPSERT_FLAG
    , SHA2(CONCAT(HOM_COV_ID, REL_COV_CD,'|'), 256) AS HASH_KEY  -- Concatenate all key columns for hashing

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

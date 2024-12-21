

{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    post_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.tdb2wplq_appl_prfll_asmt_rslt_transient', false, '.*WescomParquet/TDB2WPLQ.APPL_PRFLL_ASMT_RSLT/.*\.parquet') }}" ,
         "UPDATE {{ this }} SET CREATED_DATETIME = CURRENT_TIMESTAMP(), CREATED_BY = CURRENT_USER() WHERE ingestdate > (SELECT COALESCE(MAX(CREATED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})"
    ]
    
) }}


--,post_hook="DROP TABLE IF EXISTS bronze_zone_dev.sasreference.dbo_reqdriver_copy;"
-- Your model's main SQL query (the logic that will execute after the pre-hook)
select * 
from 
{{ source('wescompldev', 'tdb2wplq_appl_prfll_asmt_rslt_transient') }}
   

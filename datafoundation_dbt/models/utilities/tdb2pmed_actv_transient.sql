

{{ config(
    schema = 'wescom_pl',
    materialized = 'table',
    post_hook= [
        "{{ copy_into_macro('bronze_zone_dev.wescom_pl.wescom_pl_stage', 'bronze_zone_dev.wescom_pl.tdb2pmed_actv_transient', false, '.*WescomParquet/TDB2PMED.ACTV/.*\.parquet') }}" ,
         "UPDATE {{ this }} SET CREATED_DATETIME = CURRENT_TIMESTAMP(), CREATED_BY = CURRENT_USER() WHERE ingestdate > (SELECT COALESCE(MAX(CREATED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})"
    ]
    
) }}


select * 
from 
{{ source('wescompldev', 'tdb2pmed_actv_transient') }}


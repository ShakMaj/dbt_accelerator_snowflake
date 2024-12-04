

{{ config(
  
    materialized = 'table',
    post_hook= [
        "{{ copy_into_macro('sasreference_stage', 'bronze_zone_dev.sasreference.dbo_reqdriver_transient', false, '.*test3/dbo.REQDRIVER/.*\.parquet') }}" ,
         "UPDATE {{ this }} SET CREATED_DATETIME = CURRENT_TIMESTAMP(), CREATED_BY = CURRENT_USER() WHERE ingestdate > (SELECT COALESCE(MAX(CREATED_DATETIME), TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) FROM {{ this }})"
    ]
    
) }}


--,post_hook="DROP TABLE IF EXISTS bronze_zone_dev.sasreference.dbo_reqdriver_copy;"
-- Your model's main SQL query (the logic that will execute after the pre-hook)
select * 
from 
dbo_reqdriver_transient
   

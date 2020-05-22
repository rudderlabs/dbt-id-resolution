/*

Version correctness check test. Min of CURR should be greater than max of PREV

*/

with 
    min_curr_versions as 
    (
        select 
            min(version_anon_id) as min_curr_version_anon_id,
            min(version_user_id) as min_curr_version_user_id 
        from
            DATABASE.SCHEMA.DBT_ID_GRAPH_CURR    
    ),
    max_prev_versions as
    (
        select 
            max(version_anon_id) as max_prev_version_anon_id,
            max(version_user_id) as max_prev_version_user_id  
        from
            DATABASE.SCHEMA.DBT_ID_GRAPH_PREV    

    )

select 
    min_curr_version_anon_id,
    max_prev_version_anon_id,
    min_curr_version_user_id,
    max_prev_version_user_id  
from 
    min_curr_versions, max_prev_versions 
where 
    max_prev_version_anon_id >= min_curr_version_anon_id
or
    max_prev_version_user_id >= min_curr_version_user_id


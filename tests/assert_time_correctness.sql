/*

Time correctness check. MIN of CURR should be greater than MAX of PREV

*/

with 
    min_curr_time as 
    (
        select 
            min(edge_timestamp) as min_curr_edge_timestamp            
        from
            DATABASE.SCHEMA.DBT_ID_GRAPH_CURR    
    ),
    max_prev_time as
    (
        select 
            max(edge_timestamp) as max_prev_edge_timestamp 
        from
            DATABASE.SCHEMA.DBT_ID_GRAPH_PREV    

    )

select 
    min_curr_edge_timestamp,
    max_prev_edge_timestamp  
from 
    min_curr_time, max_prev_time 
where 
    max_prev_edge_timestamp >= min_curr_edge_timestamp


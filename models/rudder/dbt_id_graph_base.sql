{{
    config(
        materialized='table'
    )
}}

select 
    anonymous_id as orig_anon_id
    , user_id as orig_user_id
    , anonymous_id as curr_anon_id
    , user_id as curr_user_id
    , 0 as version_anon_id
    , 0 as version_user_id
    , cast(sent_at as timestamp) as edge_timestamp
from
    {{ source("<schema>","IDENTIFIES") }}
where
    anonymous_id is not null
and
    user_id is not null

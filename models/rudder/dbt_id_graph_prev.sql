{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert'
    )
}}

-- depends_on: {{ ref('dbt_id_graph_base') }}
/*

For first time run, copy everything from base. Incremental runs should take everthing from _CURR.
Complete re-write every time

*/

{% if not is_incremental() %}
    select * from {{ ref('dbt_id_graph_base') }}
{% endif %}    


{% if is_incremental() %}
    select 
        orig_anon_id,
        orig_user_id,
        curr_anon_id,
        curr_user_id,
        version_anon_id,
        version_user_id,
        cast(edge_timestamp as timestamp) as edge_timestamp 
    from 
        DATABASE.SCHEMA.DBT_ID_GRAPH_CURR
{% endif %}
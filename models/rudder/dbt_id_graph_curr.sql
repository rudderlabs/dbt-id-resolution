{{
    config(
        materialized='incremental'
    )
}}

-- depends_on: {{ ref('dbt_id_graph_latest_curr') }}
-- depends_on: {{ ref('dbt_id_graph_prev') }}
/*
In the incremental mode, simply add new edges from IDENTIFIES to the CURR table
*/
{% if is_incremental() %}
    
   WITH    version_table as 
            (
                SELECT 
                    max(version_anon_id)+1 as next_version_anon_id,
                    max(version_user_id)+1 as next_version_user_id
                FROM 
                    DATABASE.SCHEMA.DBT_ID_GRAPH_CURR

            )
    select
            anonymous_id as orig_anon_id
            , user_id as orig_user_id
            , anonymous_id as curr_anon_id
            , user_id as curr_user_id
            ,  version_table.next_version_anon_id as version_anon_id
            ,  version_table.next_version_user_id as version_user_id
            , cast(sent_at as timestamp) as edge_timestamp
    from 
        DATABASE.SCHEMA.IDENTIFIES,
        version_table
    where 
        cast(sent_at as timestamp) > (select max(edge_timestamp) from {{ this }})

{% endif %}

/*
Complete recreation. Here again two situations are possible - recreation at the time of first run or recreation during regular update
*/
{% if not is_incremental() %}

    /*
    Below code is to check for existence of table. If does not exists, then project is being run for first time
    */

    {%- set check_relation = adapter.get_relation(
      database="RUDDER_WEBAPP_DATA",
      schema="RUDDERWEBAPP",
      identifier="DBT_ID_GRAPH_CURR") 
    -%}    

    {% if check_relation == None %}
        select * from {{ ref('dbt_id_graph_base') }}
    {% endif %}

    /*
    Table exists which means this will be a rebuild based on latest current union prev
    */
    {% if check_relation != None %}
        select * from {{ ref('dbt_id_graph_latest_curr') }} union all select * from {{ ref('dbt_id_graph_prev')}}
    {% endif %}
{% endif %}
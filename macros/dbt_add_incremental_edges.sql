/*

This macro should be executed at regular intervals to keep the CURR (current) table updated 
with the latest records that have been added to the IDENTIFIES table. These are essentially the 
"edges" i.e. the connections between the anonymous and the user ids. 

Care should be taken to insert only those records which have been added to the IDENTIFIES table
after the last update of the CURR table. Hence only those records from IDENTIFIES are considered
which have been entered after the highest edge timestamp in the CURR table.

Please replace DATABASE and SCHEMA with the names of database and schema applicable in your case.

*/



{% macro dbt_add_incremental_edges() %}

    {% set next_version_anon_id = run_query('select max(version_anon_id) from DATABASE.SCHEMA.DBT_ID_GRAPH_CURR').columns[0].values()[0] + 1 %}
    {% set next_version_user_id = run_query('select max(version_user_id) from DATABASE.SCHEMA.RUDDERWEBAPP.DBT_ID_GRAPH_CURR').columns[0].values()[0] + 1 %}

    {% set sql %}

        insert into DATABASE.SCHEMA.DBT_ID_GRAPH_CURR
            (
                orig_anon_id
                , orig_user_id
                , curr_anon_id
                , curr_user_id
                , version_anon_id
                , version_user_id
                , edge_timestamp
            )

        select
                anonymous_id
                , user_id
                , anonymous_id
                , user_id
                , {{ next_version_anon_id }}
                , {{ next_version_user_id }}
                , cast(sent_at as timestamp)
        from 
            DATABASE.SCHEMA.IDENTIFIES
        where 
            cast(sent_at as timestamp) > (select max(edge_timestamp) from DATABASE.SCHEMA.DBT_ID_GRAPH_CURR)

    {% endset %}

    {% set table = run_query(sql) %}

{% endmacro %}

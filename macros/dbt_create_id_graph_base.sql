/*

This code is to be executed at the very beginning of setting up the ID Resolution system OR if the system needs 
to be rebuilt on account of some source table or field name getting modified. If a rebuild is required, then 
the macro should be executed as follows

dbt run-operation dbt_create_id_graph_base --args '{rebuild: true}'

Please replace DATABASE and SCHEMA in below code with your database and schema names respectively.

The sequence of functions performed by this macro is as follows
- In case of rebuild drop the BASE(base) and the CURR(current) tables. 
  The BASE table is where the first version of anonymous_id to user_id mapping is created using data
  from IDENTIFIES table of Rudder schema
  
- Create the BASE table from the IDENTIFIES table. In BASE, version should be 0. Timestamp of record
  creation is maintained. It will be used to identify incremental records as shall be seen subsequently
  
- At the very beginning, BASE and CURR would be the same

*/


{% macro dbt_create_id_graph_base(rebuild) %}

    {% if rebuild %}

        {% set sql %}

            drop table if exists DATABASE.SCHEMA.DBT_ID_GRAPH_BASE

        {% endset %}

        {% set table = run_query(sql) %}

        {% set sql %}

            drop table if exists DATABASE.SCHEMA.DBT_ID_GRAPH_CURR

        {% endset %}

        {% set table = run_query(sql) %}

    {% endif %}

    {% set sql %}

        create table if not exists DATABASE.SCHEMA.DBT_ID_GRAPH_BASE as
            select 
                anonymous_id as orig_anon_id
                , user_id as orig_user_id
                , anonymous_id as curr_anon_id
                , user_id as curr_user_id
                , 0 as version_anon_id
                , 0 as version_user_id
                , cast(sent_at as timestamp) as edge_timestamp
            from
                DATABASE.SCHEMA.IDENTIFIES
            where
                anonymous_id is not null
            and
                user_id is not null

    {% endset %}

    {% set table = run_query(sql) %}

        {% set sql %}

        create table if not exists DATABASE.SCHEMA.DBT_ID_GRAPH_CURR as
            select 
                anonymous_id as orig_anon_id
                , user_id as orig_user_id
                , anonymous_id as curr_anon_id
                , user_id as curr_user_id
                , 0 as version_anon_id
                , 0 as version_user_id
                , cast(sent_at as timestamp) as edge_timestamp
            from
                DATABASE.SCHEMA.IDENTIFIES
            where
                anonymous_id is not null
            and
                user_id is not null

    {% endset %}

    {% set table = run_query(sql) %}

{% endmacro %}

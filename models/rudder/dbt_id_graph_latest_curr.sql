/*

In this case, even incremental update requires re-write of entire table

*/

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert'
    )
}}

    /*

    For the first run, simply copy everything from base because no run happened yet

    */

    {% if not is_incremental() %}
        select * from {{ ref('dbt_id_graph_base') }}
    {% endif %}


    /*

    Incremental run. Execute the Id resolution logic

    */
    
    {% if is_incremental() %}
        WITH version_table as 
            (
                SELECT 
                    max(version_anon_id)+1 as max_version_anon_id,
                    max(version_user_id)+1 as max_version_user_id,
                    max(version_anon_id) as curr_version_anon_id,
                    max(version_user_id) as curr_version_user_id
                FROM 
                    DATABASE.SCHEMA.DBT_ID_GRAPH_CURR

            ),
            dbt_id_graph_prev as (
                SELECT * from DATABASE.SCHEMA.DBT_ID_GRAPH_CURR
            )

            (SELECT DISTINCT
                orig_anon_id,

                orig_user_id,

                CASE
                WHEN curr_anon_id IS NULL THEN NULL
                WHEN tmp_anon_id < curr_anon_id THEN tmp_anon_id
                ELSE curr_anon_id
                END AS curr_anon_id,

                CASE
                WHEN curr_user_id IS NULL THEN NULL
                WHEN tmp_user_id < curr_user_id THEN tmp_user_id
                ELSE curr_user_id
                END AS curr_user_id,

                version_table.max_version_anon_id AS version_anon_id,
                version_table.max_version_anon_id AS  version_user_id,
                
                cast(CURRENT_TIMESTAMP() as timestamp) as edge_timestamp

            FROM   
                version_table,
                (SELECT orig_anon_id,
                    orig_user_id,
                    curr_anon_id,
                    curr_user_id,
                    version_anon_id,
                    version_user_id,
                    Min(curr_user_id)
                    over(
                        PARTITION BY orig_anon_id) AS tmp_anon_id,
                    Min(curr_anon_id)
                    over(
                        PARTITION BY orig_user_id) AS tmp_user_id
                FROM   
                    (SELECT * FROM dbt_id_graph_prev UNION ALL SELECT * FROM DATABASE.SCHEMA.DBT_ID_GRAPH_BASE) AS TMP_GRAPH_IN,
                    version_table
                WHERE   orig_anon_id IN (SELECT orig_anon_id
                            FROM   dbt_id_graph_prev
                            WHERE  version_anon_id = version_table.curr_version_anon_id)
                    OR orig_user_id IN (SELECT orig_user_id
                                FROM   dbt_id_graph_prev
                                WHERE  version_user_id = version_table.curr_version_user_id)) AS
                TMP_GRAPH_OUTER
            )
    {% endif %}
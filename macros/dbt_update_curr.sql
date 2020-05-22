/*

This macro should be run at periodic intervals after executing the dbt_add_incremental_edges macro.

This macro describes an iteration of the ID resolution logic as described in 
https://rudderstack.com/blog/identity-graph-and-identity-resolution-in-sql/

CURR represents the latest status of resolution of IDs. PREV represents the state on top of which
CURR has been built.

So before performing the actual calculation, the current "version" (which represents the number of 
resolution iterations the ID has undergone) and the next version are determined.

Then the PREV table is replaced by the CURR table since what was CURR now becomes PREV.

Subsequently, the logic as described in the blog mentioned above is executed.

Please replace DATABASE and SCHEMA with the database and schema names as applicable in your case.

*/


{% macro dbt_update_curr() %}

    {% set max_version_anon_id = run_query('select max(version_anon_id) from DATABASE.SCHEMA.DBT_ID_GRAPH_CURR').columns[0].values()[0] + 1 %}
    {% set max_version_user_id = run_query('select max(version_user_id) from DATABASE.SCHEMA.DBT_ID_GRAPH_CURR').columns[0].values()[0] + 1 %}
    {% set curr_version_anon_id = run_query('select max(version_anon_id) from DATABASE.SCHEMA.DBT_ID_GRAPH_CURR').columns[0].values()[0] %}
    {% set curr_version_user_id = run_query('select max(version_user_id) from DATABASE.SCHEMA.DBT_ID_GRAPH_CURR').columns[0].values()[0] %}

    {% set sql %}

        CREATE OR REPLACE TABLE DATABASE.SCHEMA.DBT_ID_GRAPH_PREV AS
        SELECT * FROM DATABASE.SCHEMA.DBT_ID_GRAPH_CURR

    {% endset %}

    {% set table = run_query(sql) %} 

    {% set sql %}

        CREATE OR REPLACE TABLE DATABASE.SCHEMA.DBT_ID_GRAPH_CURR AS
        
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

			  {{max_version_anon_id}} AS version_anon_id,
			  {{max_version_anon_id}} AS  version_user_id,
			  
			  CURRENT_TIMESTAMP() as edge_timestamp

		   FROM   (SELECT orig_anon_id,
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
                    (SELECT * FROM DATABASE.SCHEMA.DBT_ID_GRAPH_PREV UNION ALL SELECT * FROM DATABASE.SCHEMA.DBT_ID_GRAPH_BASE) AS TMP_GRAPH_IN
			   WHERE   orig_anon_id IN (SELECT orig_anon_id
						   FROM   DATABASE.SCHEMA.DBT_ID_GRAPH_PREV
						   WHERE  version_anon_id = {{ curr_version_anon_id }})
				   OR orig_user_id IN (SELECT orig_user_id
						       FROM   DATABASE.SCHEMA.DBT_ID_GRAPH_PREV
						       WHERE  version_user_id = {{ curr_version_user_id }})) AS
			  TMP_GRAPH_OUTER
		  )

    {% endset %}

    {% set table = run_query(sql) %} 


{% endmacro %}

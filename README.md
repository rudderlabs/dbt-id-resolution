# Identity Resolution using DBT and RudderStack

RudderStack supports different data warehouse destinations such as Redshift, BigQuery, and Snowflake. For each of these warehouses, certain predefined RudderStack tables get created, along with the tables for each type of event routed to RudderStack from different sources. This project leverages the `identifies` table that is created when any client application invokes the `identify` API of the RudderStack SDK. This API is typically invoked at the time of user login or registration.

This repository contains a sample DBT project for **ID Resolution** in  a RudderStack-based Snowflake Data Warehouse.

**Note**: This project is based on the ID Resolution strategy and algorithms as described in our RudderStack blog on [**Identity Resolution**](https://rudderstack.com/blog/identity-graph-and-identity-resolution-in-sql/)

# What is ID Resolution?
At a high level, ID Resolution is explained as follows:
- An end-user can navigate a site or application anonymously, i.e. without logging in. The user would then be associated or identified with an `anonymous_id`.
- At some point, the user might be required to register or login to perform some operation. A `user_id` would then get associated with that user.
- The user’s web or in-app activity might be scattered across multiple sessions, where sometimes he may be logged in, and sometimes not. They might also be accessing the site or the application from different devices.
- **ID Resolution** ties together all these different IDs to enable the enterprise or the developer to relate all the sessions and activities to a single user.

# What Special DBT Features we use in this Project
We use the [**DBT incremental models**](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models/) for the following reasons:
- The ID linkages are built iteratively using two base tables. This process involves self-referencing
- The data from the `identifies` table is incrementally introduced into the mix to accommodate new IDs

We also use the [**DBT adapter**](https://docs.getdbt.com/docs/writing-code-in-dbt/jinja-context/adapter/) and supported functions like `check_relation`.

# How to Use this Repository
This project was created on the [DBT Cloud](https://cloud.getdbt.com). Hence, there is no `profiles.yml` file with the connection information. If you wish to execute the models in Command Line Interface (CLI) mode, you will need to create additional configuration files as documented [here](https://docs.getdbt.com/docs/running-a-dbt-project/using-the-command-line-interface/).

## Sequence of Commands

The following is the sequence of commands to be run after setting up the project for the first time, or when a complete **data rebuild** is required:

- ```dbt run --full-refresh --models dbt_id_graph_base```
- ```dbt run --full-refresh --models dbt_id_graph_prev```
- ```dbt run --full-refresh --models dbt_id_graph_latest_curr```
- ```dbt run --full-refresh --models dbt_id_graph_curr```

**Note: When doing a rebuild, all tables should be manually dropped first**

The following commands may be run at regular intervals, in the sequence mentioned below:

- ```dbt run --models dbt_id_graph_curr```			
- ```dbt run --models dbt_id_graph_prev```
- ```dbt run --models dbt_id_graph_latest_curr```
- ```dbt run --full-refresh --models dbt_id_graph_curr```

Some important points to note:

- At the end of each run cycle, the table `<DATABASE>.<SCHEMA>.DBT_ID_GRAPH_CURR` will have the latest ID linkages
- Some tests have been provided in this repository, which can be run to ensure that the tables being cycled are in a consistent state.
- Executing `dbt test --data` will execute the tests
- Remember to change DATABASE to your database name and SCHEMA to your schema name wherever applicable

# More Information
 
For more information on ID Resolution or using the DBT macros, please feel free to [contact us](https://rudderstack.com/contact/) or start a conversation on our [Discord](https://discordapp.com/invite/xNEdEGw) channel. We will be happy to help you.

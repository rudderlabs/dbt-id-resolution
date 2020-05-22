# Identity Resolution DBT Code for RudderStack

RudderStack supports different data warehouse destinations such as Redshift, BigQuery, and Snowflake. For each of these warehouses, certain predefined RudderStack tables get created, along with the tables for each type of event routed to RudderStack from different sources. This project leverages the `identifies` table that gets created when any client application invokes the `identify` API of the RudderStack SDK. This API is typically invoked at the time of user login or registration.

This repository contains a sample DBT project for **ID Resolution** in  a RudderStack-based Snowflake Data Warehouse.

**Note**: This project is based on the ID Resolution strategy and algorithms as described in our RudderStack blog on [**Identity Resolution**](https://rudderstack.com/blog/identity-graph-and-identity-resolution-in-sql/)

# What is ID Resolution?
At a high level, ID Resolution is explained as follows:
- An end-user can navigate a site or application anonymously, i.e. without logging in. The user would then be associated or identified with an `anonymous_id`.
- At some point, the user might be required to register or login to perform some operation. A `user_id` would then get associated with that user.
- The user’s web or in-app activity might be scattered across multiple sessions, where sometimes he may be logged in, and sometimes not. They might also be accessing the site or the application from different devices.
- **ID Resolution** ties together all these different IDs to enable the enterprise or the developer to relate all the sessions and activities to a single user.

# Why We Use DBT Macros in this Project
The following are the reasons for preferring macros over DBT models:
- The ID linkages are built iteratively using two base tables. The process involves self-referencing.
- Data from the `identifies` table is incrementally introduced into the mix to accommodate new IDs
- Given the need to implement the above strategies - using macros is a technically feasible option for optimal performance

# How to Use this Repository
This project was created on the [DBT Cloud](https://cloud.getdbt.com). Hence, there is no `profiles.yml` file with the connection information. If you wish to execute the models in Command Line Interface (CLI) mode, you will need to create additional configuration files as documented [here](https://docs.getdbt.com/docs/running-a-dbt-project/using-the-command-line-interface/)

## Sequence of Commands

- `dbt run-operation dbt_create_id_graph_base '{rebuild: true}'` 

	This command is to be run only once at the beginning of setting up the project. It may be run again in case any fundamental flaw on the client-side application is identified, resulting in incorrect data getting populated in the first place
- `dbt run-operation dbt_add_incremental_edges`
	This command should be run periodically to add new registration/login data to the linkages
- `dbt run-operation dbt_update_curr`
	This command should be run periodically after running `dbt_add_incremental_edges`

Some important points to note:

- At the end of each run cycle, the table `<DATABASE>.<SCHEMA>.DBT_ID_GRAPH_CURR` will have the latest ID linkages
- Some tests have been provided in this repository, which can be run to ensure that the tables being cycled are in a consistent state.
- Executing `dbt test --data` will execute the tests

# More Information
 
For more information on ID Resolution or using the DBT macros, please feel free to [contact us](https://rudderstack.com/contact/) or start a conversation on our [Discord](https://discordapp.com/invite/xNEdEGw) channel. We will be happy to help you.

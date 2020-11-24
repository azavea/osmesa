## SQL Definitions

This directory contains files with SQL definitions to set up a fresh OSMesa stats database.  The files in this top level are the definitions for the required tables which are constructed by the batch ingest process and subsequently updated by the streaming tasks.  The SQL files in the `materialized_views` directory are used to create aggregated summaries of the fundamental stats that are more useful for direct consumption by a user.  These materialized views will not, however, automatically update; they will need to be refreshed periodically.

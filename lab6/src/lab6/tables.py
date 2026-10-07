"""Small helpers reused by multiple Lab 6 notebooks."""
ANALYTICAL_TABLES = (
    "dim_wiki", "dim_date", "dim_namespace", "dim_page", "dim_editor", "fact_edits",
    "agg_wiki_5min", "agg_wiki_daily", "agg_page_activity",
)
WIKI_TABLES = tuple(t for t in ANALYTICAL_TABLES if t != "dim_date")
ALL_TABLES = ("ref_sitematrix_raw", "sec_user_access", "ops_volume_log") + ANALYTICAL_TABLES


def sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def replace_table(spark, config, name, frame):
    view = "_lab6_write_" + name
    frame.createOrReplaceTempView(view)
    # REPLACE preserves table policies and grants on Databricks.
    spark.sql(f"CREATE OR REPLACE TABLE {config.table(name)} USING DELTA AS SELECT * FROM {view}")
    spark.catalog.dropTempView(view)


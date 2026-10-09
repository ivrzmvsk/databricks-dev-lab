"""Small table-write helper reused by snapshot and DQ notebooks."""


def replace(df, table):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

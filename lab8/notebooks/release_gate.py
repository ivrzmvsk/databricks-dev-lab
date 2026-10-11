# Databricks notebook source
# MAGIC %md
# MAGIC # Verify deliveries and record the deployed commit against a pinned Silver version

# COMMAND ----------
import json
import sys

from pyspark.sql import functions as F

dbutils.widgets.text("code_root", "")
for path in ("lab5/src", "lab6/src", "lab8/src"):
    sys.path.insert(0, dbutils.widgets.get("code_root") + "/" + path)
from lab6.config import from_widgets
from lab6.security import assert_policies
from lab8.contracts import check_delivery_counts
from lab8.releases import check_repeatability

config = from_widgets(dbutils)
source = config.catalog + "." + config.source_schema
bronze = spark.table(source + ".wiki_bronze_lab5")
silver = spark.table(config.source)
quarantine = spark.table(source + ".wiki_quarantine_lab5")
counts = {"bronze": bronze.count(), "silver": silver.count(), "quarantine": quarantine.count()}
deliveries = bronze.select("kafka_topic", "kafka_partition", "kafka_offset").distinct().count()
check_delivery_counts(**counts, unique_deliveries=deliveries)
assert_policies(spark, config)

# COMMAND ----------
# MAGIC %md
# MAGIC ## A changing live source may produce more rows on the next run
# MAGIC Compare the same Silver version and build inputs. The checksum is probabilistic.

# COMMAND ----------
fact = spark.table(config.table("fact_edits"))
version = int(spark.sql(f"SHOW TBLPROPERTIES {config.table('fact_edits')} ('lab6.source_version')").first()[1])
fingerprint = spark.sql(f"SHOW TBLPROPERTIES {config.table('fact_edits')} ('lab8.build_fingerprint')").first()[1]
if len(fingerprint) != 64 or any(c not in '0123456789abcdef' for c in fingerprint):
    raise ValueError('Gold has no valid build fingerprint; rebuild it before the release gate')
checksum = fact.agg(F.sum(F.xxhash64(*fact.columns).cast("decimal(38,0)"))).first()[0]
dbutils.widgets.text("release_commit", "local")
dbutils.widgets.text("run_id", "")
run_id = dbutils.widgets.get("run_id")
if not run_id:
    raise ValueError("run_id must be supplied by the promotion Job")
measurement = spark.table(config.table("ops_volume_log")).filter(F.col("run_id") == run_id).first()
assert measurement is not None and not measurement.should_alert, "Gold volume dropped below the healthy baseline"
commit = dbutils.widgets.get("release_commit")
release_table = config.table("ops_releases")
build_table = config.table('ops_release_builds')
prior = spark.table(release_table).join(spark.table(build_table), 'run_id').filter(
    (F.col('source_version') == version) & (F.col('build_fingerprint') == fingerprint)).collect()
fact_count = fact.count()
check_repeatability(prior, version, fingerprint, fact_count, checksum)
row = (run_id, commit, version, counts["bronze"], counts["silver"], counts["quarantine"],
       fact_count, checksum, "SUCCEEDED", fingerprint)
frame = spark.createDataFrame([row],
    "run_id STRING, release_commit STRING, source_version LONG, bronze_count LONG, "
    "silver_count LONG, quarantine_count LONG, fact_count LONG, fact_checksum DECIMAL(38,0), "
    "status STRING, build_fingerprint STRING")
frame.select('run_id', 'build_fingerprint').createOrReplaceTempView('_lab8_build')
frame.drop('build_fingerprint').withColumn("checked_at", F.current_timestamp()).createOrReplaceTempView("_lab8_release")
spark.sql(f"""MERGE INTO {build_table} t USING _lab8_build s ON t.run_id = s.run_id
    WHEN NOT MATCHED THEN INSERT *""")
spark.sql(f"""MERGE INTO {release_table} t USING _lab8_release s ON t.run_id = s.run_id
    WHEN NOT MATCHED THEN INSERT *""")
result = {"run_id": run_id, "release_commit": commit, "source_version": version, 'build_fingerprint': fingerprint,
          "counts": counts, "fact_count": fact_count, "fact_checksum": str(checksum), "status": "SUCCEEDED"}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))

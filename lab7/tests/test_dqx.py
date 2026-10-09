from lab7.constraints import BRONZE_SCHEMA
from lab7.dqx_checks import apply_predicates, apply_trusted, failures_by_rule, classify_source
from lab7.quality import classify, rank_deliveries, reconcile_deliveries
from lab7.fixtures import demo_rows


def test_dqx_quarantine_preserves_contract_errors_and_raw_json(spark, dqx_engine):
    source = spark.createDataFrame(
        demo_rows(),
        BRONZE_SCHEMA,
    )
    checked = classify_source(source, dqx_engine)
    bad = checked.filter("_errors IS NOT NULL")
    assert bad.count() == 6
    assert checked.filter("_errors IS NULL").count() == 3
    assert bad.filter("size(_quality_errors) = 0").count() == 0
    assert bad.filter("NOT array_contains(_quality_errors, _errors[0].name)").count() == 0
    assert checked.select("event_json").exceptAll(source.select("event_json")).count() == 0
    pure = classify(source).select("kafka_offset", "_quality_errors")
    actual = checked.select("kafka_offset", "_quality_errors")
    assert pure.exceptAll(actual).count() == 0
    accepted = rank_deliveries(checked.filter("_errors IS NULL"))
    silver = accepted.filter("_delivery = 1")
    duplicates = accepted.filter("_delivery > 1")
    assert silver.count() == 2
    assert duplicates.count() == 1
    assert reconcile_deliveries(source, [silver, bad, duplicates], ["kafka_offset"]) == {
        "missing_rows": 0,
        "extra_rows": 0,
    }


def test_dqx_sql_null_is_rejected(spark, dqx_engine):
    checked = apply_predicates(
        spark.createDataFrame([(None,), (0,), (1,)], "value INT"),
        {"positive": "value > 0"},
        dqx_engine,
    )
    assert failures_by_rule(checked) == {"positive": 2}


def test_dqx_detects_duplicates_missing_keys_and_orphans(spark, dqx_engine):
    fact = spark.createDataFrame(
        [("same", "known"), ("same", "known"), ("orphan", "absent"), (None, None)],
        "event_id STRING, wiki_key STRING",
    )
    reference = spark.createDataFrame([("known",)], "wiki_key STRING")
    checked = apply_trusted(
        fact,
        {"keys_present": "event_id IS NOT NULL AND wiki_key IS NOT NULL"},
        {"dim_wiki": "wiki_key"},
        {"dim_wiki": reference},
        dqx_engine,
    )
    assert failures_by_rule(checked) == {
        "event_id_unique": 2,
        "keys_present": 1,
        "dim_wiki_reference": 1,
    }
    assert checked.filter("_errors IS NOT NULL").count() == 4


def test_dqx_accepts_consistent_unique_fact(spark, dqx_engine):
    fact = spark.createDataFrame(
        [("a", "known"), ("b", "known")], "event_id STRING, wiki_key STRING"
    )
    reference = spark.createDataFrame([("known",)], "wiki_key STRING")
    checked = apply_trusted(
        fact,
        {"keys_present": "event_id IS NOT NULL AND wiki_key IS NOT NULL"},
        {"dim_wiki": "wiki_key"},
        {"dim_wiki": reference},
        dqx_engine,
    )
    assert failures_by_rule(checked) == {}
    assert checked.filter("_errors IS NULL").count() == 2

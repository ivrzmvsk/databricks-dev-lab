from lab7.constraints import BRONZE_SCHEMA, FACT_NOT_NULL_COLUMNS, empty_fact_schema, fact_ddl
from lab7.fixtures import demo_rows
from lab7.quality import classify, make_fact


def test_setup_schema_matches_transformed_data_without_pipeline_tables(spark):
    expected = empty_fact_schema(spark)
    actual = make_fact(classify(spark.createDataFrame(demo_rows(), BRONZE_SCHEMA))).schema
    assert [(f.name, f.dataType) for f in expected] == [(f.name, f.dataType) for f in actual]


def test_setup_ddl_enforces_required_keys_and_preserves_optional_measures(spark):
    schema = empty_fact_schema(spark)
    columns = fact_ddl(schema).split(", ")
    not_null = {column.split("`")[1] for column in columns if column.endswith(" NOT NULL")}
    assert not_null == FACT_NOT_NULL_COLUMNS
    assert "`old_length` bigint" in columns
    assert "`bytes_delta` bigint" in columns

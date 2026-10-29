# tests/test_schema_validator.py
"""
Unit tests for SchemaValidator validating the 4 production scenarios:
1. Oracle UPPERCASE -> PostgreSQL lowercase column mapping
2. Extra source columns / missing target columns
3. Zero matched columns -> fast LoaderValidationError (< 1s)
4. Duplicate case-folded mapping -> fast DuplicateColumnMappingError
"""
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType, DecimalType, ShortType
from src.load.schema_validator import SchemaValidator, LoaderValidationError, DuplicateColumnMappingError


def test_oracle_uppercase_to_postgres_lowercase(spark):
    """Scenario 1: Oracle UPPERCASE -> PostgreSQL lowercase matching and renaming."""
    data = [("101", "Alice", 500)]
    df = spark.createDataFrame(data, ["EMIGRANT_ID", "FULL_NAME", "AMOUNT"])
    
    target_schema = [
        StructField("emigrant_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("amount", IntegerType(), True),
    ]

    aligned_df, matching_cols = SchemaValidator.validate_and_align_schema(
        df=df,
        target_schema_fields=target_schema,
        source_table="TBL_MEMBER",
        target_fqn="land.tbl_member"
    )

    assert matching_cols == ["emigrant_id", "full_name", "amount"]
    assert aligned_df.columns == ["emigrant_id", "full_name", "amount"]
    assert aligned_df.count() == 1


def test_extra_source_column_ignored(spark):
    """Scenario 2: Extra source columns are safely ignored and logged in diagnostics."""
    data = [("101", "Alice", "extra_val")]
    df = spark.createDataFrame(data, ["EMIGRANT_ID", "FULL_NAME", "EXTRA_COL"])
    
    target_schema = [
        StructField("emigrant_id", StringType(), True),
        StructField("full_name", StringType(), True),
    ]

    aligned_df, matching_cols = SchemaValidator.validate_and_align_schema(
        df=df,
        target_schema_fields=target_schema,
        source_table="TBL_MEMBER",
        target_fqn="land.tbl_member"
    )

    assert matching_cols == ["emigrant_id", "full_name"]
    assert aligned_df.columns == ["emigrant_id", "full_name"]


def test_zero_matched_columns_raises_fast_validation_error(spark):
    """Scenario 3: Zero matched columns raises LoaderValidationError immediately."""
    data = [("101", "Alice")]
    df = spark.createDataFrame(data, ["FOO", "BAR"])
    
    target_schema = [
        StructField("baz", StringType(), True),
        StructField("qux", StringType(), True),
    ]

    with pytest.raises(LoaderValidationError) as excinfo:
        SchemaValidator.validate_and_align_schema(
            df=df,
            target_schema_fields=target_schema,
            source_table="TBL_MEMBER",
            target_fqn="land.tbl_member"
        )

    assert "Zero columns matched" in str(excinfo.value)
    assert "TBL_MEMBER" in str(excinfo.value)


def test_duplicate_case_folded_columns_raises_error(spark):
    """Scenario 4: Duplicate columns after case folding (e.g. 'ID' and 'id') raise DuplicateColumnMappingError."""
    data = [(1, 2)]
    df = spark.createDataFrame(data, ["ID", "id"])

    target_schema = [
        StructField("id", IntegerType(), True),
    ]

    with pytest.raises(DuplicateColumnMappingError) as excinfo:
        SchemaValidator.validate_and_align_schema(
            df=df,
            target_schema_fields=target_schema,
            source_table="TBL_MEMBER",
            target_fqn="land.tbl_member"
        )

    assert "Duplicate column name detected" in str(excinfo.value)


def test_empty_dataframe_raises_loader_validation_error(spark):
    """Verify empty DataFrame (0 columns) fails validation immediately."""
    from pyspark.sql.types import StructType
    df = spark.createDataFrame([], StructType([]))

    with pytest.raises(LoaderValidationError) as excinfo:
        SchemaValidator.validate_dataframe(df, "TBL_MEMBER")

    assert "has ZERO columns" in str(excinfo.value)


def test_permissive_alignment_casts_integral_decimal_to_bigint_without_loss(spark):
    from decimal import Decimal

    df = spark.createDataFrame(
        [(Decimal("123.0000000000"),)],
        StructType([StructField("emigrant_id", DecimalType(38, 10), True)]),
    )

    aligned_df, matching_cols = SchemaValidator.validate_and_align_schema(
        df=df,
        target_schema_fields=[StructField("emigrant_id", LongType(), True)],
        source_table="events",
        target_fqn="landing.events",
        evolution_policy="PERMISSIVE",
    )

    assert matching_cols == ["emigrant_id"]
    assert aligned_df.schema["emigrant_id"].dataType == LongType()
    assert aligned_df.first()["emigrant_id"] == 123


def test_permissive_alignment_rejects_fractional_decimal_to_bigint(spark):
    from decimal import Decimal

    df = spark.createDataFrame(
        [(Decimal("123.5000000000"),)],
        StructType([StructField("emigrant_id", DecimalType(38, 10), True)]),
    )

    with pytest.raises(LoaderValidationError, match="lossless numeric cast"):
        SchemaValidator.validate_and_align_schema(
            df=df,
            target_schema_fields=[StructField("emigrant_id", LongType(), True)],
            source_table="events",
            target_fqn="landing.events",
            evolution_policy="PERMISSIVE",
        )


def test_permissive_alignment_casts_int_to_smallint_within_range(spark):
    df = spark.createDataFrame(
        [(1, 100)],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("status", IntegerType(), True),
        ]),
    )

    aligned_df, matching_cols = SchemaValidator.validate_and_align_schema(
        df=df,
        target_schema_fields=[
            StructField("id", IntegerType(), True),
            StructField("status", ShortType(), True),
        ],
        source_table="districts",
        target_fqn="land_n.districts",
        evolution_policy="PERMISSIVE",
    )

    assert matching_cols == ["id", "status"]
    assert aligned_df.schema["status"].dataType == ShortType()
    assert aligned_df.first()["status"] == 100


def test_permissive_alignment_rejects_int_to_smallint_overflow(spark):
    df = spark.createDataFrame(
        [(1, 50000)],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("status", IntegerType(), True),
        ]),
    )

    with pytest.raises(LoaderValidationError, match="lossless integer cast is not possible"):
        SchemaValidator.validate_and_align_schema(
            df=df,
            target_schema_fields=[
                StructField("id", IntegerType(), True),
                StructField("status", ShortType(), True),
            ],
            source_table="districts",
            target_fqn="land_n.districts",
            evolution_policy="PERMISSIVE",
        )


def test_permissive_alignment_casts_smallint_to_int(spark):
    df = spark.createDataFrame(
        [(1, 10)],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("status", ShortType(), True),
        ]),
    )

    aligned_df, matching_cols = SchemaValidator.validate_and_align_schema(
        df=df,
        target_schema_fields=[
            StructField("id", IntegerType(), True),
            StructField("status", IntegerType(), True),
        ],
        source_table="districts",
        target_fqn="land_n.districts",
        evolution_policy="PERMISSIVE",
    )

    assert matching_cols == ["id", "status"]
    assert aligned_df.schema["status"].dataType == IntegerType()


def test_permissive_alignment_casts_boolean_to_smallint(spark):
    df = spark.createDataFrame(
        [(1, True)],
        StructType([
            StructField("id", IntegerType(), True),
            StructField("is_active", BooleanType(), True),
        ]),
    )

    aligned_df, matching_cols = SchemaValidator.validate_and_align_schema(
        df=df,
        target_schema_fields=[
            StructField("id", IntegerType(), True),
            StructField("is_active", ShortType(), True),
        ],
        source_table="users",
        target_fqn="land_n.users",
        evolution_policy="PERMISSIVE",
    )

    assert matching_cols == ["id", "is_active"]
    assert aligned_df.schema["is_active"].dataType == ShortType()


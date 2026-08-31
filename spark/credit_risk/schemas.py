"""Explicit Spark schemas for source, feature and audit records."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MONEY = DecimalType(18, 2)
RATIO = DecimalType(18, 6)

APPLICATION_SCHEMA = StructType(
    [
        StructField("loan_id", StringType(), nullable=False),
        StructField("borrower_id", StringType(), nullable=False),
        StructField("application_timestamp", TimestampType(), nullable=False),
        StructField("loan_amount", MONEY, nullable=False),
        StructField("loan_term_months", IntegerType(), nullable=False),
        StructField("interest_rate", DecimalType(7, 4), nullable=False),
        StructField("loan_purpose", StringType(), nullable=False),
    ]
)

BORROWER_PROFILE_SCHEMA = StructType(
    [
        StructField("borrower_id", StringType(), nullable=False),
        StructField("annual_income", MONEY, nullable=False),
        StructField("employment_years", IntegerType(), nullable=False),
        StructField("credit_score", IntegerType(), nullable=False),
        StructField("existing_debt", MONEY, nullable=False),
        StructField("home_ownership", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=False),
    ]
)

LOAN_OUTCOME_SCHEMA = StructType(
    [
        StructField("loan_id", StringType(), nullable=False),
        StructField("outcome_date", DateType(), nullable=False),
        StructField("defaulted", IntegerType(), nullable=False),
    ]
)

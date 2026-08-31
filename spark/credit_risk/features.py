"""Feature expressions shared by training and scoring pipelines."""

from __future__ import annotations

from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

FEATURE_VERSION: Final = "v1"

# Identity and policy-sensitive audit fields (including age) are deliberately excluded.
MODEL_FEATURE_ALLOWLIST: Final = (
    "annual_income",
    "employment_years",
    "credit_score",
    "existing_debt",
    "home_ownership",
    "loan_amount",
    "loan_term_months",
    "interest_rate",
    "loan_purpose",
    "debt_to_income",
    "loan_to_income",
    "estimated_monthly_payment",
    "payment_to_income",
    "credit_score_band",
    "employment_stability",
)


def build_feature_frame(joined: DataFrame, *, mode: str) -> DataFrame:
    """Derive model features with Spark expressions only; no Python UDFs."""

    if mode not in {"training", "scoring"}:
        raise ValueError(f"Unsupported feature mode: {mode}")
    if mode == "scoring" and "defaulted" in joined.columns:
        raise ValueError("Scoring input must not contain the outcome label")

    monthly_rate = F.col("interest_rate").cast("double") / F.lit(1200.0)
    term = F.col("loan_term_months").cast("double")
    principal = F.col("loan_amount").cast("double")
    compound = F.pow(F.lit(1.0) + monthly_rate, term)
    monthly_payment = principal * monthly_rate * compound / (compound - F.lit(1.0))

    featured = (
        joined.withColumn(
            "debt_to_income",
            (F.col("existing_debt") / F.col("annual_income")).cast(DecimalType(18, 6)),
        )
        .withColumn(
            "loan_to_income",
            (F.col("loan_amount") / F.col("annual_income")).cast(DecimalType(18, 6)),
        )
        .withColumn("estimated_monthly_payment", monthly_payment.cast(DecimalType(18, 2)))
        .withColumn(
            "payment_to_income",
            (monthly_payment / (F.col("annual_income").cast("double") / F.lit(12.0))).cast(
                DecimalType(18, 6)
            ),
        )
        .withColumn(
            "credit_score_band",
            F.when(F.col("credit_score") < 580, F.lit("POOR"))
            .when(F.col("credit_score") < 670, F.lit("FAIR"))
            .when(F.col("credit_score") < 740, F.lit("GOOD"))
            .when(F.col("credit_score") < 800, F.lit("VERY_GOOD"))
            .otherwise(F.lit("EXCEPTIONAL")),
        )
        .withColumn(
            "employment_stability",
            F.when(F.col("employment_years") < 2, F.lit("NEW"))
            .when(F.col("employment_years") < 5, F.lit("ESTABLISHED"))
            .otherwise(F.lit("STABLE")),
        )
        .withColumn("feature_version", F.lit(FEATURE_VERSION))
    )

    output_columns = [
        "loan_id",
        "borrower_id",
        "feature_date",
        "batch_id",
        "feature_version",
        *MODEL_FEATURE_ALLOWLIST,
    ]
    if mode == "training":
        output_columns.append("defaulted")
    return featured.select(*output_columns)

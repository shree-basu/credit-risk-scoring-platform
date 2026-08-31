"""Batch invariants, row quarantine and feature-population reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .features import build_feature_frame


class BatchQualityError(RuntimeError):
    """A non-retryable source-batch invariant failed."""


@dataclass(frozen=True)
class Reconciliation:
    input_count: int
    accepted_count: int
    quarantined_count: int


@dataclass
class FeatureBuildResult:
    accepted: DataFrame
    quarantine: DataFrame
    reconciliation: Reconciliation

    def release(self) -> None:
        self.accepted.unpersist()
        self.quarantine.unpersist()


def _assert_unique(frame: DataFrame, key: str, entity: str) -> None:
    duplicate_exists = (
        frame.where(F.col(key).isNotNull())
        .groupBy(key)
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_exists:
        raise BatchQualityError(f"DUPLICATE_{entity.upper()}_{key.upper()}")


def _reason_code() -> F.Column:
    return F.concat_ws(
        "|",
        F.when(F.col("loan_id").isNull(), "MISSING_LOAN_ID"),
        F.when(F.col("borrower_id").isNull(), "MISSING_BORROWER_ID"),
        F.when(F.col("profile_borrower_id").isNull(), "MISSING_BORROWER_PROFILE"),
        F.when(F.col("loan_amount").isNull() | (F.col("loan_amount") <= 0), "INVALID_LOAN_AMOUNT"),
        F.when(
            F.col("loan_term_months").isNull()
            | ~F.col("loan_term_months").isin(12, 24, 36, 48, 60),
            "INVALID_LOAN_TERM",
        ),
        F.when(
            F.col("interest_rate").isNull()
            | (F.col("interest_rate") <= 0)
            | (F.col("interest_rate") > 100),
            "INVALID_INTEREST_RATE",
        ),
        F.when(
            F.col("annual_income").isNull() | (F.col("annual_income") <= 0),
            "INVALID_ANNUAL_INCOME",
        ),
        F.when(
            F.col("credit_score").isNull() | ~F.col("credit_score").between(300, 850),
            "INVALID_CREDIT_SCORE",
        ),
        F.when(
            F.col("existing_debt").isNull() | (F.col("existing_debt") < 0),
            "INVALID_EXISTING_DEBT",
        ),
        F.when(
            F.col("employment_years").isNull() | (F.col("employment_years") < 0),
            "INVALID_EMPLOYMENT_YEARS",
        ),
        F.when(F.col("application_timestamp").isNull(), "INVALID_APPLICATION_TIMESTAMP"),
        F.when(F.col("loan_purpose").isNull(), "INVALID_LOAN_PURPOSE"),
    )


def _add_training_reasons(frame: DataFrame) -> DataFrame:
    training_reason = F.concat_ws(
        "|",
        F.when(F.col("reason_code") != "", F.col("reason_code")),
        F.when(F.col("outcome_loan_id").isNull(), "MISSING_LOAN_OUTCOME"),
        F.when(F.col("defaulted").isNull() | ~F.col("defaulted").isin(0, 1), "INVALID_LABEL"),
        F.when(F.col("outcome_date").isNull(), "INVALID_OUTCOME_DATE"),
        F.when(F.col("outcome_date") > F.col("feature_date"), "OUTCOME_AFTER_TRAINING_CUTOFF"),
    )
    return frame.withColumn("reason_code", training_reason)


def build_validated_features(
    applications: DataFrame,
    borrower_profiles: DataFrame,
    *,
    mode: str,
    feature_date: date,
    batch_id: str,
    run_id: str,
    loan_outcomes: DataFrame | None = None,
) -> FeatureBuildResult:
    """Create accepted features and one quarantine record per rejected application."""

    if mode not in {"training", "scoring"}:
        raise ValueError(f"Unsupported mode: {mode}")
    if mode == "training" and loan_outcomes is None:
        raise ValueError("Training mode requires loan outcomes")
    if mode == "scoring" and loan_outcomes is not None:
        raise ValueError("Scoring mode forbids loan outcomes")

    _assert_unique(applications, "loan_id", "applications")
    _assert_unique(borrower_profiles, "borrower_id", "borrower_profiles")
    if loan_outcomes is not None:
        _assert_unique(loan_outcomes, "loan_id", "loan_outcomes")

    profiles = borrower_profiles.select(
        F.col("borrower_id").alias("profile_borrower_id"),
        "annual_income",
        "employment_years",
        "credit_score",
        "existing_debt",
        "home_ownership",
        "age",
    )
    joined = (
        applications.join(
            profiles,
            applications.borrower_id == profiles.profile_borrower_id,
            "left",
        )
        .withColumn("feature_date", F.lit(feature_date.isoformat()).cast("date"))
        .withColumn("batch_id", F.lit(batch_id))
    )
    if loan_outcomes is not None:
        outcomes = loan_outcomes.select(
            F.col("loan_id").alias("outcome_loan_id"), "outcome_date", "defaulted"
        )
        joined = joined.join(
            outcomes,
            joined.loan_id == outcomes.outcome_loan_id,
            "left",
        )

    evaluated = joined.withColumn("reason_code", _reason_code())
    if mode == "training":
        evaluated = _add_training_reasons(evaluated)

    valid_rows = evaluated.where(F.col("reason_code") == "")
    invalid_rows = evaluated.where(F.col("reason_code") != "")
    accepted = build_feature_frame(valid_rows, mode=mode).persist(StorageLevel.MEMORY_AND_DISK)
    quarantine = invalid_rows.select(
        F.lit(run_id).alias("run_id"),
        F.lit(batch_id).alias("batch_id"),
        F.lit(feature_date.isoformat()).cast("date").alias("business_date"),
        F.lit("feature_join").alias("entity"),
        "loan_id",
        "reason_code",
        F.col("reason_code").alias("reason_text"),
        F.current_timestamp().alias("rejected_at"),
    ).persist(StorageLevel.MEMORY_AND_DISK)

    input_count = applications.count()
    accepted_count = accepted.count()
    quarantined_count = quarantine.count()
    if input_count != accepted_count + quarantined_count:
        accepted.unpersist()
        quarantine.unpersist()
        raise BatchQualityError(
            "FEATURE_RECONCILIATION_FAILED: "
            f"{input_count} != {accepted_count} + {quarantined_count}"
        )
    return FeatureBuildResult(
        accepted=accepted,
        quarantine=quarantine,
        reconciliation=Reconciliation(input_count, accepted_count, quarantined_count),
    )

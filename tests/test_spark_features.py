from __future__ import annotations

from argparse import Namespace
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from spark.credit_risk.features import FEATURE_VERSION, MODEL_FEATURE_ALLOWLIST
from spark.credit_risk.io import deterministic_staging_table
from spark.credit_risk.job import validate_cloud_gates
from spark.credit_risk.quality import BatchQualityError, build_validated_features
from spark.credit_risk.schemas import (
    APPLICATION_SCHEMA,
    BORROWER_PROFILE_SCHEMA,
    LOAN_OUTCOME_SCHEMA,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("credit-risk-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _application(loan_id: str, borrower_id: str) -> tuple[object, ...]:
    return (
        loan_id,
        borrower_id,
        datetime(2025, 1, 1, 12, tzinfo=timezone.utc),  # noqa: UP017
        Decimal("12000.00"),
        36,
        Decimal("12.0000"),
        "car",
    )


def _profile(borrower_id: str, income: str = "60000.00") -> tuple[object, ...]:
    return (
        borrower_id,
        Decimal(income),
        6,
        710,
        Decimal("10000.00"),
        "RENT",
        37,
    )


def _outcome(loan_id: str, outcome_date: date, defaulted: int = 0) -> tuple[object, ...]:
    return loan_id, outcome_date, defaulted


def test_training_features_are_reconciled_and_use_no_python_udf(spark: SparkSession) -> None:
    applications = spark.createDataFrame(
        [_application("L1", "B1"), _application("L2", "B2")], APPLICATION_SCHEMA
    )
    profiles = spark.createDataFrame([_profile("B1"), _profile("B2")], BORROWER_PROFILE_SCHEMA)
    outcomes = spark.createDataFrame(
        [_outcome("L1", date(2025, 12, 1), 0), _outcome("L2", date(2025, 12, 1), 1)],
        LOAN_OUTCOME_SCHEMA,
    )

    result = build_validated_features(
        applications,
        profiles,
        loan_outcomes=outcomes,
        mode="training",
        feature_date=date(2026, 1, 1),
        batch_id="batch-1",
        run_id="run-1",
    )
    try:
        assert result.reconciliation.input_count == 2
        assert result.reconciliation.accepted_count == 2
        assert result.reconciliation.quarantined_count == 0
        assert "defaulted" in result.accepted.columns
        assert "age" not in result.accepted.columns
        row = result.accepted.where("loan_id = 'L1'").first()
        assert row.feature_version == FEATURE_VERSION
        assert float(row.debt_to_income) == pytest.approx(1 / 6, rel=1e-5)
        assert float(row.loan_to_income) == pytest.approx(0.2, rel=1e-5)
        assert row.credit_score_band == "GOOD"
        assert "PythonUDF" not in result.accepted._jdf.queryExecution().executedPlan().toString()
    finally:
        result.release()


def test_scoring_features_have_no_label_or_sensitive_age(spark: SparkSession) -> None:
    result = build_validated_features(
        spark.createDataFrame([_application("L1", "B1")], APPLICATION_SCHEMA),
        spark.createDataFrame([_profile("B1")], BORROWER_PROFILE_SCHEMA),
        mode="scoring",
        feature_date=date(2026, 1, 2),
        batch_id="batch-2",
        run_id="run-2",
    )
    try:
        assert "defaulted" not in result.accepted.columns
        assert "age" not in result.accepted.columns
        assert set(MODEL_FEATURE_ALLOWLIST).issubset(result.accepted.columns)
    finally:
        result.release()


def test_invalid_and_unmatched_rows_are_quarantined_once(spark: SparkSession) -> None:
    applications = spark.createDataFrame(
        [_application("L1", "B1"), _application("L2", "B2")], APPLICATION_SCHEMA
    )
    profiles = spark.createDataFrame([_profile("B1", income="0.00")], BORROWER_PROFILE_SCHEMA)

    result = build_validated_features(
        applications,
        profiles,
        mode="scoring",
        feature_date=date(2026, 1, 2),
        batch_id="batch-2",
        run_id="run-2",
    )
    try:
        assert result.reconciliation.input_count == 2
        assert result.reconciliation.accepted_count == 0
        assert result.reconciliation.quarantined_count == 2
        reasons = {row.loan_id: row.reason_code for row in result.quarantine.collect()}
        assert "INVALID_ANNUAL_INCOME" in reasons["L1"]
        assert "MISSING_BORROWER_PROFILE" in reasons["L2"]
    finally:
        result.release()


def test_duplicate_loan_is_a_permanent_batch_failure(spark: SparkSession) -> None:
    applications = spark.createDataFrame(
        [_application("L1", "B1"), _application("L1", "B1")], APPLICATION_SCHEMA
    )
    profiles = spark.createDataFrame([_profile("B1")], BORROWER_PROFILE_SCHEMA)

    with pytest.raises(BatchQualityError, match="DUPLICATE_APPLICATIONS_LOAN_ID"):
        build_validated_features(
            applications,
            profiles,
            mode="scoring",
            feature_date=date(2026, 1, 2),
            batch_id="batch-2",
            run_id="run-2",
        )


def test_outcome_after_cutoff_is_quarantined(spark: SparkSession) -> None:
    result = build_validated_features(
        spark.createDataFrame([_application("L1", "B1")], APPLICATION_SCHEMA),
        spark.createDataFrame([_profile("B1")], BORROWER_PROFILE_SCHEMA),
        loan_outcomes=spark.createDataFrame(
            [_outcome("L1", date(2026, 2, 1), 1)], LOAN_OUTCOME_SCHEMA
        ),
        mode="training",
        feature_date=date(2026, 1, 1),
        batch_id="batch-1",
        run_id="run-1",
    )
    try:
        assert result.reconciliation.quarantined_count == 1
        assert "OUTCOME_AFTER_TRAINING_CUTOFF" in result.quarantine.first().reason_code
    finally:
        result.release()


def test_staging_name_is_deterministic_and_cloud_gates_fail_closed() -> None:
    assert deterministic_staging_table("Batch-1", FEATURE_VERSION, "scoring") == (
        deterministic_staging_table("Batch-1", FEATURE_VERSION, "scoring")
    )
    arguments = Namespace(
        input_mode="local",
        batch_uri="gs://bucket/raw",
        confirm_cloud_read="",
        output_mode="local",
        confirm_cloud_write="",
    )
    with pytest.raises(ValueError, match="gs:// input"):
        validate_cloud_gates(arguments)

    arguments.batch_uri = "data/output/raw"
    arguments.output_mode = "bigquery"
    with pytest.raises(ValueError, match="BigQuery output"):
        validate_cloud_gates(arguments)

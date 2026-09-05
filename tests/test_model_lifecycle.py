from __future__ import annotations

import json
from datetime import UTC, date, datetime

import numpy as np
import pandas as pd
import pytest

from credit_risk.feature_contract import MODEL_FEATURE_ALLOWLIST
from monitoring.drift import build_drift_metric, population_stability_index
from vertex.governance import candidate_from_metadata, promote_candidate
from vertex.predictor.app import create_app
from vertex.registry import upload_candidate
from vertex.trainer.train import (
    TrainingConfig,
    read_training_frame,
    train_candidate,
    write_artifacts,
)


def training_frame(size: int = 120) -> pd.DataFrame:
    rows = []
    for index in range(size):
        risky = index % 3 == 0
        income = 35_000 + (index % 12) * 2_500
        debt = 24_000 if risky else 4_000
        loan = 28_000 if risky else 8_000
        rows.append(
            {
                "loan_id": f"LN-{index:04d}",
                "borrower_id": f"BR-{index:04d}",
                "age": 20 + index % 50,
                "feature_version": "v1",
                "annual_income": income,
                "employment_years": 1 if risky else 8,
                "credit_score": 540 if risky else 760,
                "existing_debt": debt,
                "loan_amount": loan,
                "loan_term_months": 60 if risky else 24,
                "interest_rate": 18.0 if risky else 7.0,
                "debt_to_income": debt / income,
                "loan_to_income": loan / income,
                "estimated_monthly_payment": loan / (60 if risky else 24),
                "payment_to_income": (loan / (60 if risky else 24)) / (income / 12),
                "home_ownership": "RENT" if risky else "OWN",
                "loan_purpose": "DEBT_CONSOLIDATION" if risky else "HOME_IMPROVEMENT",
                "credit_score_band": "POOR" if risky else "VERY_GOOD",
                "employment_stability": "NEW" if risky else "STABLE",
                "defaulted": int(risky),
            }
        )
    return pd.DataFrame(rows)


def config() -> TrainingConfig:
    return TrainingConfig(
        model_version="model-2026-09-01",
        training_snapshot=date(2026, 8, 31),
        min_roc_auc=0.0,
        min_average_precision=0.0,
    )


def test_training_is_deterministic_and_uses_only_allowlisted_features() -> None:
    frame = training_frame()
    first = train_candidate(frame, config())
    second = train_candidate(frame, config())

    sample = frame.loc[:9, MODEL_FEATURE_ALLOWLIST]
    np.testing.assert_allclose(
        first.model.predict_proba(sample), second.model.predict_proba(sample), rtol=0, atol=0
    )
    assert first.metrics == second.metrics
    assert first.metadata["status"] == "CANDIDATE"
    assert first.metadata["feature_allowlist"] == list(MODEL_FEATURE_ALLOWLIST)
    assert set(first.metadata["identity_fields_excluded"]) == {"loan_id", "borrower_id", "age"}
    assert set(first.metrics) == {
        "roc_auc",
        "average_precision",
        "precision_at_threshold",
        "recall_at_threshold",
        "brier_score",
    }


def test_training_rejects_invalid_label_contract() -> None:
    frame = training_frame()
    frame["defaulted"] = 0
    with pytest.raises(ValueError, match="both binary classes"):
        train_candidate(frame, config())


def test_training_rejects_feature_contract_mismatch() -> None:
    frame = training_frame()
    frame.loc[0, "feature_version"] = "v2"
    with pytest.raises(ValueError, match="feature_version=v1"):
        train_candidate(frame, config())


def test_bigquery_training_input_fails_before_cloud_import() -> None:
    with pytest.raises(RuntimeError, match="confirm-cloud-read BIGQUERY"):
        read_training_frame(
            source="never-used.credit_risk_features.training_features",
            input_mode="bigquery",
            project_id="never-used",
            batch_id="training-20260831",
            training_snapshot=date(2026, 8, 31),
            confirmation=None,
        )


def test_artifact_round_trip_and_vertex_compatible_prediction(tmp_path) -> None:
    result = train_candidate(training_frame(), config())
    write_artifacts(result, tmp_path)
    assert json.loads((tmp_path / "metadata.json").read_text())["status"] == "CANDIDATE"

    client = create_app(tmp_path).test_client()
    assert client.get("/health").status_code == 200
    instance = training_frame(1).iloc[0].to_dict()
    response = client.post("/predict", json={"instances": [instance]})
    assert response.status_code == 200
    prediction = response.get_json()["predictions"][0]
    assert 0.0 <= prediction["probability_of_default"] <= 1.0
    assert prediction["risk_band"] in {"LOW", "MEDIUM", "HIGH"}
    assert prediction["model_version"] == "model-2026-09-01"
    assert "decision" not in prediction


def test_prediction_rejects_missing_feature(tmp_path) -> None:
    write_artifacts(train_candidate(training_frame(), config()), tmp_path)
    response = (
        create_app(tmp_path)
        .test_client()
        .post("/predict", json={"instances": [{"loan_id": "LN-1"}]})
    )
    assert response.status_code == 400
    assert "missing features" in response.get_json()["error"]

    instance = training_frame(1).iloc[0].to_dict()
    instance["feature_version"] = "v2"
    response = create_app(tmp_path).test_client().post("/predict", json={"instances": [instance]})
    assert response.status_code == 400
    assert "does not match" in response.get_json()["error"]


def test_promotion_is_explicit_and_audited() -> None:
    result = train_candidate(training_frame(), config())
    candidate = candidate_from_metadata(result.metadata)
    with pytest.raises(RuntimeError, match="PROMOTE"):
        promote_candidate(
            candidate,
            confirmation="no",
            model_resource="projects/example/locations/us-central1/models/123@2",
            promoted_by="owner@example.test",
            effective_from=datetime(2026, 9, 1, tzinfo=UTC),
        )
    active = promote_candidate(
        candidate,
        confirmation="PROMOTE",
        model_resource="projects/example/locations/us-central1/models/123@2",
        promoted_by="owner@example.test",
        effective_from=datetime(2026, 9, 1, tzinfo=UTC),
    )
    assert active.status == "ACTIVE"
    assert active.model_resource.endswith("@2")
    assert active.promoted_by == "owner@example.test"


def test_vertex_upload_gate_fails_before_cloud_import() -> None:
    with pytest.raises(RuntimeError, match="confirmation token VERTEX"):
        upload_candidate(
            project="never-used",
            location="us-central1",
            display_name="never-used",
            artifact_uri="gs://never-used/model",
            serving_image_uri="never-used/image",
            model_version="never-used",
            feature_version="v1",
            training_snapshot="2026-08-31",
            metrics_uri="gs://never-used/model/metrics.json",
            confirmation="",
        )


def test_numeric_and_categorical_drift_are_transparent() -> None:
    reference = pd.Series(np.arange(100, dtype=float))
    assert population_stability_index(reference, reference.copy()) == pytest.approx(0.0)

    metric = build_drift_metric(
        feature_name="credit_score",
        reference=reference,
        current=pd.Series(np.arange(100, dtype=float) + 500),
        reference_period=date(2026, 7, 31),
        current_period=date(2026, 8, 31),
        measured_at=datetime(2026, 9, 1, tzinfo=UTC),
        metric="PSI",
        threshold=0.2,
    )
    assert metric.status == "ALERT"
    assert metric.metric_value >= metric.threshold

    mean_shift = build_drift_metric(
        feature_name="credit_score",
        reference=reference,
        current=pd.Series(np.arange(100, dtype=float) + 50),
        reference_period=date(2026, 7, 31),
        current_period=date(2026, 8, 31),
        measured_at=datetime(2026, 9, 1, tzinfo=UTC),
        metric="STANDARDIZED_MEAN_SHIFT",
        threshold=1.0,
    )
    assert mean_shift.status == "ALERT"

    categorical = build_drift_metric(
        feature_name="home_ownership",
        reference=pd.Series(["OWN"] * 90 + ["RENT"] * 10),
        current=pd.Series(["OWN"] * 10 + ["RENT"] * 90),
        reference_period=date(2026, 7, 31),
        current_period=date(2026, 8, 31),
        measured_at=datetime(2026, 9, 1, tzinfo=UTC),
        metric="TOTAL_VARIATION",
        threshold=0.2,
    )
    assert categorical.status == "ALERT"

"""Deterministic local trainer compatible with Vertex AI custom training."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from credit_risk.feature_contract import (
    CATEGORICAL_FEATURES,
    FEATURE_VERSION,
    MODEL_FEATURE_ALLOWLIST,
    NUMERIC_FEATURES,
)

ARTIFACT_FILENAMES = ("model.joblib", "metadata.json", "metrics.json")


@dataclass(frozen=True)
class TrainingConfig:
    model_version: str
    training_snapshot: date
    random_state: int = 42
    test_size: float = 0.25
    decision_threshold: float = 0.5
    min_roc_auc: float = 0.55
    min_average_precision: float = 0.20


@dataclass(frozen=True)
class TrainingResult:
    model: Pipeline
    metadata: dict[str, Any]
    metrics: dict[str, float]


def _validate_training_frame(frame: pd.DataFrame) -> None:
    required = {*MODEL_FEATURE_ALLOWLIST, "feature_version", "defaulted"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"Training frame is missing required columns: {missing}")
    labels = set(frame["defaulted"].dropna().unique().tolist())
    if labels != {0, 1}:
        raise ValueError("Training labels must contain both binary classes 0 and 1")
    if len(frame) < 20:
        raise ValueError("Training requires at least 20 accepted examples")
    feature_versions = set(frame["feature_version"].dropna().astype(str).unique().tolist())
    if feature_versions != {FEATURE_VERSION}:
        raise ValueError(
            f"Training frame must contain only feature_version={FEATURE_VERSION}; "
            f"found {sorted(feature_versions)}"
        )


def _pipeline(config: TrainingConfig) -> Pipeline:
    numeric = Pipeline(
        [
            ("impute", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
        ]
    )
    categorical = Pipeline(
        [
            ("impute", SimpleImputer(strategy="most_frequent")),
            ("encode", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    preprocessing = ColumnTransformer(
        [
            ("numeric", numeric, list(NUMERIC_FEATURES)),
            ("categorical", categorical, list(CATEGORICAL_FEATURES)),
        ]
    )
    classifier = LogisticRegression(
        class_weight="balanced",
        max_iter=1000,
        random_state=config.random_state,
        solver="liblinear",
    )
    return Pipeline([("preprocess", preprocessing), ("classifier", classifier)])


def train_candidate(frame: pd.DataFrame, config: TrainingConfig) -> TrainingResult:
    """Train and evaluate a candidate; never promote it automatically."""

    _validate_training_frame(frame)
    features = frame.loc[:, MODEL_FEATURE_ALLOWLIST].copy()
    labels = frame["defaulted"].astype(int)
    train_x, test_x, train_y, test_y = train_test_split(
        features,
        labels,
        test_size=config.test_size,
        random_state=config.random_state,
        stratify=labels,
    )
    model = _pipeline(config)
    model.fit(train_x, train_y)
    probability = model.predict_proba(test_x)[:, 1]
    predicted = (probability >= config.decision_threshold).astype(int)
    metrics = {
        "roc_auc": float(roc_auc_score(test_y, probability)),
        "average_precision": float(average_precision_score(test_y, probability)),
        "precision_at_threshold": float(precision_score(test_y, predicted, zero_division=0)),
        "recall_at_threshold": float(recall_score(test_y, predicted, zero_division=0)),
        "brier_score": float(brier_score_loss(test_y, probability)),
    }
    passed = (
        metrics["roc_auc"] >= config.min_roc_auc
        and metrics["average_precision"] >= config.min_average_precision
    )
    metadata: dict[str, Any] = {
        "model_version": config.model_version,
        "feature_version": FEATURE_VERSION,
        "training_snapshot": config.training_snapshot.isoformat(),
        "status": "CANDIDATE" if passed else "REJECTED",
        "feature_allowlist": list(MODEL_FEATURE_ALLOWLIST),
        "identity_fields_excluded": ["loan_id", "borrower_id", "age"],
        "label": "defaulted",
        "decision_threshold": config.decision_threshold,
        "quality_gates": {
            "min_roc_auc": config.min_roc_auc,
            "min_average_precision": config.min_average_precision,
        },
        "random_state": config.random_state,
    }
    return TrainingResult(model=model, metadata=metadata, metrics=metrics)


def write_artifacts(result: TrainingResult, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    joblib.dump(result.model, destination / "model.joblib")
    (destination / "metadata.json").write_text(
        json.dumps(result.metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (destination / "metrics.json").write_text(
        json.dumps(result.metrics, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _upload_artifacts(local_dir: Path, destination: str, confirmation: str | None) -> None:
    if not destination.startswith("gs://"):
        write_artifacts(_load_result_for_copy(local_dir), Path(destination))
        return
    if confirmation != "GCS":
        raise RuntimeError("Cloud artifact upload requires --confirm-cloud-write GCS")
    from google.cloud import storage  # imported only after the explicit safety gate

    bucket_name, _, prefix = destination[5:].partition("/")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for filename in ARTIFACT_FILENAMES:
        object_name = f"{prefix.rstrip('/')}/{filename}" if prefix else filename
        bucket.blob(object_name).upload_from_filename(str(local_dir / filename))


def _load_result_for_copy(source: Path) -> TrainingResult:
    return TrainingResult(
        model=joblib.load(source / "model.joblib"),
        metadata=json.loads((source / "metadata.json").read_text(encoding="utf-8")),
        metrics=json.loads((source / "metrics.json").read_text(encoding="utf-8")),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Local CSV or Parquet feature snapshot")
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--training-snapshot", required=True, type=date.fromisoformat)
    parser.add_argument("--output", default=os.environ.get("AIP_MODEL_DIR", "artifacts/model"))
    parser.add_argument("--confirm-cloud-write")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    source = Path(args.input)
    frame = pd.read_parquet(source) if source.suffix == ".parquet" else pd.read_csv(source)
    config = TrainingConfig(
        model_version=args.model_version,
        training_snapshot=args.training_snapshot,
    )
    result = train_candidate(frame, config)
    local_output = (
        Path(args.output) if not args.output.startswith("gs://") else Path("artifacts/model")
    )
    write_artifacts(result, local_output)
    if args.output.startswith("gs://"):
        _upload_artifacts(local_output, args.output, args.confirm_cloud_write)
    print(json.dumps({"metadata": result.metadata, "metrics": result.metrics}, sort_keys=True))


if __name__ == "__main__":
    main()

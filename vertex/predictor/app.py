"""Vertex-compatible HTTP prediction application."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from flask import Flask, jsonify, request

from credit_risk.feature_contract import MODEL_FEATURE_ALLOWLIST

ARTIFACT_FILENAMES = ("model.joblib", "metadata.json", "metrics.json")


def risk_band(probability: float) -> str:
    if probability < 0.20:
        return "LOW"
    if probability < 0.50:
        return "MEDIUM"
    return "HIGH"


def create_app(model_dir: str | Path) -> Flask:
    """Create the app from a local artifact directory; no cloud client is constructed."""

    artifact_dir = Path(model_dir)
    model = joblib.load(artifact_dir / "model.joblib")
    metadata = json.loads((artifact_dir / "metadata.json").read_text(encoding="utf-8"))
    app = Flask(__name__)
    health_route = os.environ.get("AIP_HEALTH_ROUTE", "/health")
    predict_route = os.environ.get("AIP_PREDICT_ROUTE", "/predict")

    @app.get(health_route)
    def health() -> tuple[Any, int]:
        return jsonify({"status": "ready", "model_version": metadata["model_version"]}), 200

    @app.post(predict_route)
    def predict() -> tuple[Any, int]:
        payload = request.get_json(silent=True) or {}
        instances = payload.get("instances")
        if not isinstance(instances, list) or not instances:
            return jsonify({"error": "instances must be a non-empty list"}), 400
        for index, instance in enumerate(instances):
            if not isinstance(instance, dict):
                return jsonify({"error": f"instances[{index}] must be an object"}), 400
            missing = [name for name in MODEL_FEATURE_ALLOWLIST if name not in instance]
            if missing:
                return jsonify({"error": f"instances[{index}] missing features: {missing}"}), 400
            supplied_version = instance.get("feature_version")
            if supplied_version is not None and supplied_version != metadata["feature_version"]:
                return jsonify(
                    {
                        "error": (
                            f"instances[{index}] feature_version {supplied_version!r} does not "
                            f"match model feature_version {metadata['feature_version']!r}"
                        )
                    }
                ), 400

        features = pd.DataFrame(instances).loc[:, MODEL_FEATURE_ALLOWLIST]
        probabilities = model.predict_proba(features)[:, 1]
        predictions = []
        for instance, probability in zip(instances, probabilities, strict=True):
            predictions.append(
                {
                    "loan_id": instance.get("loan_id"),
                    "borrower_id": instance.get("borrower_id"),
                    "probability_of_default": float(probability),
                    "risk_band": risk_band(float(probability)),
                    "model_version": metadata["model_version"],
                    "feature_version": metadata["feature_version"],
                }
            )
        return jsonify({"predictions": predictions}), 200

    return app


def materialize_model_dir(uri: str) -> Path:
    """Resolve local artifacts or download Vertex's AIP_STORAGE_URI at container startup."""

    if not uri.startswith("gs://"):
        return Path(uri)
    from google.cloud import storage

    bucket_name, _, prefix = uri[5:].partition("/")
    destination = Path(tempfile.mkdtemp(prefix="credit-risk-model-"))
    bucket = storage.Client().bucket(bucket_name)
    for filename in ARTIFACT_FILENAMES:
        object_name = f"{prefix.rstrip('/')}/{filename}" if prefix else filename
        bucket.blob(object_name).download_to_filename(str(destination / filename))
    return destination


def create_runtime_app() -> Flask:
    artifact_uri = os.environ.get("AIP_STORAGE_URI", os.environ.get("MODEL_DIR", "/model"))
    return create_app(materialize_model_dir(artifact_uri))


def main() -> None:
    app = create_runtime_app()
    app.run(host="0.0.0.0", port=int(os.environ.get("AIP_HTTP_PORT", "8080")))


if __name__ == "__main__":
    main()

"""Explicitly gated Vertex Model Registry adapter."""

from __future__ import annotations

from typing import Any


def upload_candidate(
    *,
    project: str,
    location: str,
    display_name: str,
    artifact_uri: str,
    serving_image_uri: str,
    model_version: str,
    feature_version: str,
    training_snapshot: str,
    metrics_uri: str,
    confirmation: str,
    parent_model: str | None = None,
) -> Any:
    """Upload only after explicit confirmation; importing this module is cloud-free."""

    if confirmation != "VERTEX":
        raise RuntimeError("Vertex registration requires confirmation token VERTEX")
    from google.cloud import aiplatform  # imported only after the explicit gate

    aiplatform.init(project=project, location=location)
    return aiplatform.Model.upload(
        display_name=display_name,
        labels={
            "model_version": model_version.lower(),
            "feature_version": feature_version.lower(),
            "training_snapshot": f"date-{training_snapshot}",
        },
        version_description=f"Evaluation metrics: {metrics_uri}",
        artifact_uri=artifact_uri,
        serving_container_image_uri=serving_image_uri,
        serving_container_predict_route="/predict",
        serving_container_health_route="/health",
        serving_container_ports=[8080],
        parent_model=parent_model,
        is_default_version=not parent_model,
        version_aliases=["candidate"] if parent_model else None,
        sync=True,
    )

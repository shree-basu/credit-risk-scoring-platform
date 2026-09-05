"""Pure governance rules; cloud registration is a separate explicit action."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime


@dataclass(frozen=True)
class ModelAssignment:
    model_resource: str | None
    model_version: str
    feature_version: str
    status: str
    effective_from: datetime | None = None
    promoted_by: str | None = None


def candidate_from_metadata(metadata: dict[str, object]) -> ModelAssignment:
    status = str(metadata["status"])
    if status not in {"CANDIDATE", "REJECTED"}:
        raise ValueError(f"Unexpected training status: {status}")
    return ModelAssignment(
        model_resource=None,
        model_version=str(metadata["model_version"]),
        feature_version=str(metadata["feature_version"]),
        status=status,
    )


def promote_candidate(
    candidate: ModelAssignment,
    *,
    confirmation: str,
    model_resource: str,
    promoted_by: str,
    effective_from: datetime,
) -> ModelAssignment:
    """Return an active assignment only after a deliberate audited confirmation."""

    if confirmation != "PROMOTE":
        raise RuntimeError("Promotion requires confirmation token PROMOTE")
    if candidate.status != "CANDIDATE":
        raise RuntimeError("Only a quality-gated candidate can be promoted")
    if not model_resource.startswith("projects/") or "/models/" not in model_resource:
        raise ValueError("A fully qualified immutable model_resource is required")
    if not promoted_by.strip():
        raise ValueError("promoted_by is required for the audit record")
    return replace(
        candidate,
        model_resource=model_resource,
        status="ACTIVE",
        effective_from=effective_from,
        promoted_by=promoted_by,
    )

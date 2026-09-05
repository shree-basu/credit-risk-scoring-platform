"""Explicit source reads and guarded feature-publication adapters."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .quality import FeatureBuildResult
from .schemas import APPLICATION_SCHEMA, BORROWER_PROFILE_SCHEMA, LOAN_OUTCOME_SCHEMA


@dataclass(frozen=True)
class SourceFrames:
    applications: DataFrame
    borrower_profiles: DataFrame
    loan_outcomes: DataFrame | None


def read_source_frames(spark: SparkSession, batch_uri: str, mode: str) -> SourceFrames:
    """Read only the exact entity paths from the validated immutable batch."""

    if mode not in {"training", "scoring"}:
        raise ValueError(f"Unsupported mode: {mode}")
    prefix = batch_uri.rstrip("/")
    reader = spark.read.option("header", True).option("mode", "PERMISSIVE")
    applications = reader.schema(APPLICATION_SCHEMA).csv(f"{prefix}/applications.csv")
    profiles = reader.schema(BORROWER_PROFILE_SCHEMA).csv(f"{prefix}/borrower_profiles.csv")
    outcomes = None
    if mode == "training":
        outcomes = reader.schema(LOAN_OUTCOME_SCHEMA).csv(f"{prefix}/loan_outcomes.csv")
    return SourceFrames(applications, profiles, outcomes)


def write_local(result: FeatureBuildResult, output_root: Path, logical_key: str) -> None:
    """Replay-safe local publication by overwriting one deterministic logical path."""

    destination = output_root / logical_key
    result.accepted.write.mode("overwrite").parquet(str(destination / "accepted"))
    result.quarantine.write.mode("overwrite").parquet(str(destination / "quarantine"))


def deterministic_staging_table(batch_id: str, feature_version: str, mode: str) -> str:
    safe_prefix = re.sub(r"[^a-z0-9_]", "_", batch_id.lower()).strip("_")[:32] or "batch"
    digest = hashlib.sha256(f"{mode}|{batch_id}|{feature_version}".encode()).hexdigest()[:12]
    return f"{mode}_features_stage_{safe_prefix}_{digest}"


def write_bigquery_staging(
    result: FeatureBuildResult,
    *,
    project_id: str,
    feature_dataset: str,
    audit_dataset: str,
    staging_table: str,
    confirmation: str,
) -> None:
    """Write deterministic staging tables only after an explicit cloud-write gate."""

    if confirmation != "BIGQUERY":
        raise ValueError("BigQuery publication requires --confirm-cloud-write BIGQUERY")
    required = (project_id, feature_dataset, audit_dataset, staging_table)
    if not all(required):
        raise ValueError("BigQuery publication requires project, datasets and staging table")

    result.accepted.write.format("bigquery").option(
        "table", f"{project_id}.{feature_dataset}.{staging_table}"
    ).option("writeMethod", "direct").mode("overwrite").save()
    result.quarantine.write.format("bigquery").option(
        "table", f"{project_id}.{audit_dataset}.quarantine_stage_{staging_table}"
    ).option("writeMethod", "direct").mode("overwrite").save()

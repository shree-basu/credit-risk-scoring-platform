"""Local-safe entry point for training or scoring feature engineering."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from .features import FEATURE_VERSION
from .io import (
    deterministic_staging_table,
    read_source_frames,
    write_bigquery_staging,
    write_local,
)
from .quality import build_validated_features


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("training", "scoring"), required=True)
    parser.add_argument("--batch-uri", required=True)
    parser.add_argument("--feature-date", type=date.fromisoformat, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    parser.add_argument("--input-mode", choices=("local", "gcs"), default="local")
    parser.add_argument("--confirm-cloud-read", default="")
    parser.add_argument("--output-mode", choices=("local", "bigquery"), default="local")
    parser.add_argument("--output-uri", type=Path, default=Path("data/output/features"))
    parser.add_argument("--project-id", default="")
    parser.add_argument("--feature-dataset", default="credit_risk_features")
    parser.add_argument("--audit-dataset", default="credit_risk_audit")
    parser.add_argument("--confirm-cloud-write", default="")
    return parser.parse_args()


def validate_cloud_gates(args: argparse.Namespace) -> None:
    if args.input_mode == "gcs" and args.confirm_cloud_read != "GCS":
        raise ValueError("GCS input requires --confirm-cloud-read GCS")
    if args.input_mode == "local" and args.batch_uri.startswith("gs://"):
        raise ValueError("gs:// input requires --input-mode gcs and explicit confirmation")
    if args.output_mode == "bigquery" and args.confirm_cloud_write != "BIGQUERY":
        raise ValueError("BigQuery output requires --confirm-cloud-write BIGQUERY")


def main() -> None:
    args = parse_args()
    # Fail before creating Spark, loading connectors or touching any cloud endpoint.
    validate_cloud_gates(args)
    spark = (
        SparkSession.builder.appName(f"credit-risk-{args.mode}-features")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    try:
        sources = read_source_frames(spark, args.batch_uri, args.mode)
        result = build_validated_features(
            sources.applications,
            sources.borrower_profiles,
            loan_outcomes=sources.loan_outcomes,
            mode=args.mode,
            feature_date=args.feature_date,
            batch_id=args.batch_id,
            run_id=args.run_id,
        )
        try:
            logical_key = (
                f"{args.mode}/feature_date={args.feature_date.isoformat()}/"
                f"batch_id={args.batch_id}/feature_version={FEATURE_VERSION}"
            )
            if args.output_mode == "local":
                write_local(result, args.output_uri, logical_key)
            else:
                staging_table = deterministic_staging_table(
                    args.batch_id, FEATURE_VERSION, args.mode
                )
                write_bigquery_staging(
                    result,
                    project_id=args.project_id,
                    feature_dataset=args.feature_dataset,
                    audit_dataset=args.audit_dataset,
                    staging_table=staging_table,
                    confirmation=args.confirm_cloud_write,
                )
        finally:
            result.release()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

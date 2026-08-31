"""Generate deterministic, immutable training or scoring source batches."""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import random
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

from data.simulator.contracts import (
    ENTITIES_BY_DATASET,
    BatchLayout,
    validate_local_batch,
    write_manifest,
)

SCENARIOS: Final = (
    "normal",
    "duplicates",
    "missing-profile",
    "invalid-values",
    "distribution-drift",
)
PURPOSES: Final = (
    "car",
    "debt_consolidation",
    "education",
    "home_improvement",
    "medical",
    "small_business",
)
HOME_OWNERSHIP: Final = ("RENT", "MORTGAGE", "OWN")

APPLICATION_FIELDS: Final = (
    "loan_id",
    "borrower_id",
    "application_timestamp",
    "loan_amount",
    "loan_term_months",
    "interest_rate",
    "loan_purpose",
)
PROFILE_FIELDS: Final = (
    "borrower_id",
    "annual_income",
    "employment_years",
    "credit_score",
    "existing_debt",
    "home_ownership",
    "age",
)
OUTCOME_FIELDS: Final = ("loan_id", "outcome_date", "defaulted")


@dataclass(frozen=True)
class GenerationConfig:
    dataset_type: str
    partition_date: date
    batch_id: str
    records: int = 5000
    seed: int = 42
    scenario: str = "normal"
    output_dir: Path = Path("data/output")

    def __post_init__(self) -> None:
        BatchLayout(self.dataset_type, self.partition_date, self.batch_id)
        if self.records <= 0:
            raise ValueError("records must be greater than zero")
        if self.scenario not in SCENARIOS:
            raise ValueError(f"Unsupported scenario: {self.scenario}")

    @property
    def layout(self) -> BatchLayout:
        return BatchLayout(self.dataset_type, self.partition_date, self.batch_id)


def _rng(config: GenerationConfig) -> random.Random:
    identity = (
        f"{config.seed}|{config.dataset_type}|{config.partition_date.isoformat()}|"
        f"{config.batch_id}|{config.scenario}|{config.records}"
    )
    derived_seed = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big")
    return random.Random(derived_seed)


def _money(value: float) -> str:
    return f"{value:.2f}"


def _probability_of_default(profile: dict[str, object], application: dict[str, object]) -> float:
    income = float(profile["annual_income"])
    debt = float(profile["existing_debt"])
    amount = float(application["loan_amount"])
    credit_score = int(profile["credit_score"])
    interest_rate = float(application["interest_rate"])
    employment_years = int(profile["employment_years"])

    log_odds = (
        -3.1
        + 2.0 * (debt / max(income, 1.0))
        + 1.4 * (amount / max(income, 1.0))
        + 0.035 * max(0.0, interest_rate - 8.0)
        + 0.008 * max(0, 680 - credit_score)
        + 0.25 * (employment_years < 2)
    )
    return min(0.95, max(0.01, 1.0 / (1.0 + math.exp(-log_odds))))


def _application_timestamp(config: GenerationConfig, rng: random.Random) -> datetime:
    if config.dataset_type == "training":
        source_date = config.partition_date - timedelta(days=rng.randint(120, 900))
    else:
        source_date = config.partition_date
    return datetime(
        source_date.year,
        source_date.month,
        source_date.day,
        rng.randint(0, 23),
        rng.randint(0, 59),
        rng.randint(0, 59),
        tzinfo=timezone.utc,  # noqa: UP017 - keeps local Spark validation Python 3.10 compatible
    )


def build_entities(config: GenerationConfig) -> dict[str, list[dict[str, object]]]:
    rng = _rng(config)
    applications: list[dict[str, object]] = []
    profiles: list[dict[str, object]] = []
    outcomes: list[dict[str, object]] = []

    for index in range(1, config.records + 1):
        loan_id = f"LN-{config.batch_id}-{index:07d}"
        borrower_id = f"BR-{config.batch_id}-{index:07d}"
        drift = config.scenario == "distribution-drift"
        credit_score = rng.randint(300, 700 if drift else 850)
        annual_income = rng.randint(20_000, 125_000 if drift else 200_000)
        existing_debt = rng.randint(15_000 if drift else 0, 75_000 if drift else 50_000)
        profile: dict[str, object] = {
            "borrower_id": borrower_id,
            "annual_income": _money(annual_income),
            "employment_years": rng.randint(0, 40),
            "credit_score": credit_score,
            "existing_debt": _money(existing_debt),
            "home_ownership": rng.choice(HOME_OWNERSHIP),
            "age": rng.randint(21, 75),
        }
        application_timestamp = _application_timestamp(config, rng)
        application: dict[str, object] = {
            "loan_id": loan_id,
            "borrower_id": borrower_id,
            "application_timestamp": application_timestamp.isoformat().replace("+00:00", "Z"),
            "loan_amount": _money(rng.randint(1_000, 100_000)),
            "loan_term_months": rng.choice((12, 24, 36, 48, 60)),
            "interest_rate": _money(rng.uniform(3.0, 25.0)),
            "loan_purpose": rng.choice(PURPOSES),
        }
        applications.append(application)
        profiles.append(profile)

        if config.dataset_type == "training":
            probability = _probability_of_default(profile, application)
            outcomes.append(
                {
                    "loan_id": loan_id,
                    "outcome_date": config.partition_date.isoformat(),
                    "defaulted": int(rng.random() < probability),
                }
            )

    if config.scenario == "duplicates":
        applications.append(dict(applications[0]))
    elif config.scenario == "missing-profile":
        profiles.pop()
    elif config.scenario == "invalid-values":
        profiles[0]["annual_income"] = "0.00"
        applications[min(1, len(applications) - 1)]["loan_amount"] = "-100.00"

    entities = {"applications": applications, "borrower_profiles": profiles}
    if config.dataset_type == "training":
        entities["loan_outcomes"] = outcomes
    return entities


def _write_csv(path: Path, fields: Iterable[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def generate_batch(config: GenerationConfig) -> Path:
    """Create one batch atomically and refuse to overwrite an existing identity."""

    layout = config.layout
    batch_dir = config.output_dir / Path(*layout.prefix.parts)
    staging_dir = batch_dir.with_name(f".{batch_dir.name}.incomplete")
    if batch_dir.exists() or staging_dir.exists():
        raise FileExistsError(f"Immutable batch path already exists: {batch_dir}")

    batch_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir()
    try:
        entities = build_entities(config)
        _write_csv(staging_dir / "applications.csv", APPLICATION_FIELDS, entities["applications"])
        _write_csv(
            staging_dir / "borrower_profiles.csv", PROFILE_FIELDS, entities["borrower_profiles"]
        )
        if config.dataset_type == "training":
            _write_csv(staging_dir / "loan_outcomes.csv", OUTCOME_FIELDS, entities["loan_outcomes"])
        write_manifest(staging_dir, layout)
        (staging_dir / "_SUCCESS").write_text("", encoding="utf-8")
        validate_local_batch(staging_dir, layout)
        staging_dir.rename(batch_dir)
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise
    return batch_dir


def upload_batch_to_gcs(batch_dir: Path, layout: BatchLayout, bucket_name: str, token: str) -> None:
    """Upload only after an explicit irreversible cloud-action confirmation."""

    if token != "GCS":
        raise ValueError("GCS upload requires --confirm-upload GCS")
    if not bucket_name:
        raise ValueError("GCS upload requires --gcs-bucket")

    from google.cloud import storage  # type: ignore[import-not-found]

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for path in sorted(batch_dir.iterdir()):
        if path.is_file():
            bucket.blob(layout.object_path(path.name)).upload_from_filename(path)


def _parse_date(raw_value: str) -> date:
    return date.fromisoformat(raw_value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--records", type=int, default=5000)
    parser.add_argument("--business-date", type=_parse_date)
    parser.add_argument("--snapshot-date", type=_parse_date)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--dataset-type", choices=tuple(ENTITIES_BY_DATASET), required=True)
    parser.add_argument("--scenario", choices=SCENARIOS, default="normal")
    parser.add_argument("--output-dir", type=Path, default=Path("data/output"))
    parser.add_argument("--mode", choices=("local", "gcs"), default="local")
    parser.add_argument("--gcs-bucket", default="")
    parser.add_argument("--confirm-upload", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.dataset_type == "training":
        if args.snapshot_date is None or args.business_date is not None:
            raise SystemExit("training requires --snapshot-date and forbids --business-date")
        partition_date = args.snapshot_date
    else:
        if args.business_date is None or args.snapshot_date is not None:
            raise SystemExit("scoring requires --business-date and forbids --snapshot-date")
        partition_date = args.business_date

    if args.mode == "gcs" and args.confirm_upload != "GCS":
        raise SystemExit("GCS upload requires --confirm-upload GCS")
    config = GenerationConfig(
        dataset_type=args.dataset_type,
        partition_date=partition_date,
        batch_id=args.batch_id,
        records=args.records,
        seed=args.seed,
        scenario=args.scenario,
        output_dir=args.output_dir,
    )
    batch_dir = generate_batch(config)
    if args.mode == "gcs":
        upload_batch_to_gcs(batch_dir, config.layout, args.gcs_bucket, args.confirm_upload)
    print(batch_dir)


if __name__ == "__main__":
    main()

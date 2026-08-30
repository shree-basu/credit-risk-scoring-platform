"""Immutable source-batch layout and manifest validation."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path, PurePosixPath
from typing import Final

SCHEMA_VERSION: Final = "1.0.0"
ENTITIES_BY_DATASET: Final = {
    "training": ("applications", "borrower_profiles", "loan_outcomes"),
    "scoring": ("applications", "borrower_profiles"),
}


@dataclass(frozen=True)
class BatchLayout:
    """Canonical identity and paths for one immutable source batch."""

    dataset_type: str
    partition_date: date
    batch_id: str

    def __post_init__(self) -> None:
        if self.dataset_type not in ENTITIES_BY_DATASET:
            raise ValueError(f"Unsupported dataset type: {self.dataset_type}")
        allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
        if not self.batch_id or any(character not in allowed for character in self.batch_id):
            raise ValueError("batch_id may contain only letters, numbers, '-' and '_'")

    @property
    def partition_key(self) -> str:
        return "snapshot_date" if self.dataset_type == "training" else "business_date"

    @property
    def prefix(self) -> PurePosixPath:
        return PurePosixPath(
            "raw",
            self.dataset_type,
            f"{self.partition_key}={self.partition_date.isoformat()}",
            f"batch_id={self.batch_id}",
        )

    def object_path(self, filename: str) -> str:
        return str(self.prefix / filename)

    @property
    def generated_at(self) -> str:
        timestamp = datetime.combine(self.partition_date, time.min, tzinfo=timezone.utc)
        return timestamp.isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def csv_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as source:
        reader = csv.reader(source)
        next(reader, None)
        return sum(1 for _ in reader)


def build_manifest(batch_dir: Path, layout: BatchLayout) -> dict[str, object]:
    entities = []
    for entity in ENTITIES_BY_DATASET[layout.dataset_type]:
        filename = f"{entity}.csv"
        entity_path = batch_dir / filename
        entities.append(
            {
                "entity": entity,
                "object_path": layout.object_path(filename),
                "expected_row_count": csv_row_count(entity_path),
                "sha256": sha256_file(entity_path),
                "schema_version": SCHEMA_VERSION,
            }
        )
    return {
        "dataset_type": layout.dataset_type,
        "partition_key": layout.partition_key,
        "partition_date": layout.partition_date.isoformat(),
        "batch_id": layout.batch_id,
        "generated_at": layout.generated_at,
        "entities": entities,
    }


def write_manifest(batch_dir: Path, layout: BatchLayout) -> Path:
    manifest_path = batch_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(build_manifest(batch_dir, layout), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest_path


def validate_local_batch(batch_dir: Path, layout: BatchLayout) -> dict[str, object]:
    """Validate identity, exact paths, counts, checksums and completion marker."""

    manifest_path = batch_dir / "manifest.json"
    if not manifest_path.is_file() or not (batch_dir / "_SUCCESS").is_file():
        raise ValueError("Batch requires manifest.json and _SUCCESS")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected_header = {
        "dataset_type": layout.dataset_type,
        "partition_key": layout.partition_key,
        "partition_date": layout.partition_date.isoformat(),
        "batch_id": layout.batch_id,
        "generated_at": layout.generated_at,
    }
    for field, expected in expected_header.items():
        if manifest.get(field) != expected:
            raise ValueError(f"Manifest {field} does not match the requested batch")

    entries = manifest.get("entities")
    if not isinstance(entries, list):
        raise ValueError("Manifest entities must be a list")
    expected_entities = set(ENTITIES_BY_DATASET[layout.dataset_type])
    actual_entities = {entry.get("entity") for entry in entries if isinstance(entry, dict)}
    if actual_entities != expected_entities or len(entries) != len(expected_entities):
        raise ValueError("Manifest entity set does not match the source contract")

    for entry in entries:
        entity = str(entry["entity"])
        filename = f"{entity}.csv"
        path = batch_dir / filename
        if entry.get("object_path") != layout.object_path(filename):
            raise ValueError(f"Manifest object path mismatch for {entity}")
        if entry.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(f"Schema version mismatch for {entity}")
        if not path.is_file():
            raise ValueError(f"Missing source object for {entity}")
        if entry.get("expected_row_count") != csv_row_count(path):
            raise ValueError(f"Row-count mismatch for {entity}")
        if entry.get("sha256") != sha256_file(path):
            raise ValueError(f"Checksum mismatch for {entity}")
    return manifest

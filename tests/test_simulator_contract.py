from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import pytest

from data.simulator.contracts import BatchLayout, validate_local_batch
from data.simulator.generate_loans import (
    GenerationConfig,
    build_entities,
    generate_batch,
    upload_batch_to_gcs,
)


def _config(root: Path, dataset_type: str = "training", scenario: str = "normal") -> GenerationConfig:
    return GenerationConfig(
        dataset_type=dataset_type,
        partition_date=date(2026, 8, 30),
        batch_id="batch-001",
        records=12,
        seed=17,
        scenario=scenario,
        output_dir=root,
    )


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as source:
        return list(csv.DictReader(source))


def test_training_batch_is_deterministic_and_contract_valid(tmp_path: Path) -> None:
    first = generate_batch(_config(tmp_path / "one"))
    second = generate_batch(_config(tmp_path / "two"))

    assert {path.name: path.read_bytes() for path in first.iterdir()} == {
        path.name: path.read_bytes() for path in second.iterdir()
    }
    manifest = validate_local_batch(first, _config(tmp_path / "unused").layout)
    assert {entry["entity"] for entry in manifest["entities"]} == {
        "applications",
        "borrower_profiles",
        "loan_outcomes",
    }
    assert all(entry["expected_row_count"] == 12 for entry in manifest["entities"])


def test_scoring_batch_has_no_label_or_outcome_file(tmp_path: Path) -> None:
    batch_dir = generate_batch(_config(tmp_path, dataset_type="scoring"))

    assert not (batch_dir / "loan_outcomes.csv").exists()
    assert "defaulted" not in _rows(batch_dir / "applications.csv")[0]
    manifest = json.loads((batch_dir / "manifest.json").read_text(encoding="utf-8"))
    assert {entry["entity"] for entry in manifest["entities"]} == {
        "applications",
        "borrower_profiles",
    }


@pytest.mark.parametrize(
    ("scenario", "expected_application_count", "expected_profile_count"),
    [("duplicates", 13, 12), ("missing-profile", 12, 11), ("invalid-values", 12, 12)],
)
def test_controlled_bad_data_scenarios(
    tmp_path: Path,
    scenario: str,
    expected_application_count: int,
    expected_profile_count: int,
) -> None:
    entities = build_entities(_config(tmp_path, scenario=scenario))
    assert len(entities["applications"]) == expected_application_count
    assert len(entities["borrower_profiles"]) == expected_profile_count
    if scenario == "invalid-values":
        assert entities["borrower_profiles"][0]["annual_income"] == "0.00"
        assert any(row["loan_amount"] == "-100.00" for row in entities["applications"])


def test_batch_path_is_immutable(tmp_path: Path) -> None:
    config = _config(tmp_path)
    generate_batch(config)
    with pytest.raises(FileExistsError, match="Immutable batch path"):
        generate_batch(config)


def test_manifest_rejects_redirected_object_path(tmp_path: Path) -> None:
    config = _config(tmp_path)
    batch_dir = generate_batch(config)
    manifest_path = batch_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["entities"][0]["object_path"] = "raw/another-file.csv"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="object path mismatch"):
        validate_local_batch(batch_dir, config.layout)


def test_cloud_client_cannot_be_constructed_before_confirmation(tmp_path: Path) -> None:
    layout = BatchLayout("scoring", date(2026, 8, 30), "batch-001")
    with pytest.raises(ValueError, match="confirm-upload"):
        upload_batch_to_gcs(tmp_path, layout, "example-bucket", token="NO")

"""Small dependency, credential-pattern, and infrastructure-default checks."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PINNED_REQUIREMENTS = (
    "requirements.txt",
    "requirements-ml.txt",
    "requirements-spark.txt",
    "requirements-airflow.txt",
    "requirements-cloud.txt",
    "vertex/trainer/requirements.txt",
    "vertex/predictor/requirements.txt",
)


def tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [ROOT / line for line in result.stdout.splitlines() if line]


def check_dependencies() -> None:
    failures: list[str] = []
    for relative in PINNED_REQUIREMENTS:
        for number, raw_line in enumerate(
            (ROOT / relative).read_text(encoding="utf-8").splitlines(), start=1
        ):
            line = raw_line.strip()
            if not line or line.startswith("#") or line.startswith("-r "):
                continue
            if "==" not in line:
                failures.append(f"{relative}:{number}: dependency is not exactly pinned")
    if failures:
        raise SystemExit("\n".join(failures))


def check_credentials() -> None:
    patterns = {
        "private key": "-----BEGIN " + "PRIVATE KEY-----",
        "Google service-account type": '"type"' + r"\s*:\s*\"service_account\"",
        "Google private_key field": '"private_' + r"key\"\s*:",
        "AWS access key": r"AKIA[0-9A-Z]{16}",
    }
    failures: list[str] = []
    for path in tracked_files():
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".lock.hcl"}:
            continue
        content = path.read_text(encoding="utf-8", errors="ignore")
        for label, pattern in patterns.items():
            if re.search(pattern, content):
                failures.append(f"{path.relative_to(ROOT)}: possible {label}")
    if failures:
        raise SystemExit("\n".join(failures))


def check_zero_resource_defaults() -> None:
    variables = (ROOT / "infra/terraform/variables.tf").read_text(encoding="utf-8")
    example = (ROOT / "infra/terraform/dev.auto.tfvars.example").read_text(encoding="utf-8")
    if variables.count("default     = false") < 2:
        raise SystemExit("Terraform deployment and Composer defaults must remain false")
    for expected in ("deployment_enabled      = false", "enable_composer         = false"):
        if expected not in example:
            raise SystemExit(f"Missing zero-resource example default: {expected}")


def main() -> None:
    check_dependencies()
    check_credentials()
    check_zero_resource_defaults()
    print("Dependency, credential-pattern, and zero-resource safety checks passed.")


if __name__ == "__main__":
    main()

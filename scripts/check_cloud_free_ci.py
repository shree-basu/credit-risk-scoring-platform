"""Fail CI when a workflow gains a cloud authentication or deployment command."""

from __future__ import annotations

import re
from pathlib import Path

WORKFLOW_ROOT = Path(__file__).resolve().parents[1] / ".github" / "workflows"

# Assemble sensitive strings so this source file does not flag itself when the
# repository is scanned for credential/deployment patterns.
FORBIDDEN = {
    "Google authentication action": "google-github-actions/" + "auth",
    "gcloud command": "g" + "cloud ",
    "gsutil command": "gs" + "util ",
    "BigQuery CLI command": "b" + "q ",
    "Terraform plan/apply/destroy": r"terraform\s+(?:plan|apply|destroy)\b",
    "container registry push": r"docker\s+push\b",
    "build action push": r"push:\s*true\b",
}


def main() -> None:
    failures: list[str] = []
    workflows = sorted((*WORKFLOW_ROOT.glob("*.yml"), *WORKFLOW_ROOT.glob("*.yaml")))
    if not workflows:
        raise SystemExit("No GitHub Actions workflows found")

    for path in workflows:
        content = path.read_text(encoding="utf-8")
        for label, pattern in FORBIDDEN.items():
            if re.search(pattern, content, flags=re.IGNORECASE):
                failures.append(f"{path.relative_to(WORKFLOW_ROOT.parent.parent)}: {label}")

    if failures:
        raise SystemExit("Cloud-unsafe workflow content:\n" + "\n".join(failures))
    print(f"Cloud-free workflow policy passed for {len(workflows)} workflow(s).")


if __name__ == "__main__":
    main()

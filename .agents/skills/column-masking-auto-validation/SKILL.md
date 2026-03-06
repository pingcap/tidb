---
name: column-masking-auto-validation
description: Run TiDB column masking automated validation and generate a human-readable report from a fixed scenario matrix. Use when you need repeatable feature validation + report generation.
---

# Column Masking Auto Validation

## Purpose

This skill provides a repeatable workflow to validate the column masking feature and generate a human-readable report automatically.

The output is:

- one execution artifact directory (`artifacts/column-masking/<timestamp>/`)
- one markdown report with scenario-level status and evidence

## What this skill includes

- feature test plan (human-facing): `references/column-masking-test-plan.md`
- report template: `references/report-template.md`
- scenario matrix (P0): `references/p0-scenario-matrix.json`
- scenario matrix (P1): `references/p1-scenario-matrix.json`
- automation scripts:
  - `scripts/run_validation.sh`
  - `scripts/generate_report.py`

## Standard workflow

1. Run validation:

```bash
./.agents/skills/column-masking-auto-validation/scripts/run_validation.sh
```

2. For stricter CI-like checks, include prepare/lint:

```bash
./.agents/skills/column-masking-auto-validation/scripts/run_validation.sh --with-bazel-prepare --with-lint
```

3. Read generated report:

- `artifacts/column-masking/<timestamp>/column-masking-report.md`

## Optional modes

- Generate report from an existing artifacts directory:

```bash
./.agents/skills/column-masking-auto-validation/scripts/run_validation.sh --skip-tests --artifacts-dir <existing_artifacts_dir>
```

## Notes

- The script uses repository-approved test commands (targeted unit tests + integration tests).
- The final human-facing outputs are:
  - the test plan (`references/column-masking-test-plan.md`)
  - the generated report in artifacts (created every run)
- Do not maintain static conclusion documents under `docs/design` for this feature.

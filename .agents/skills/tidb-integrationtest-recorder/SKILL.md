---
name: tidb-integrationtest-recorder
description: Use when recording TiDB integration tests under tests/integrationtest and verifying regenerated result files stay minimal and correct.
---

# TiDB Integration Test Recorder

## Overview

Use this workflow for changes under `tests/integrationtest/t/**` or when SQL behavior needs integration coverage.
Do not use `-record` for this suite.
Canonical command details live in `docs/agents/testing-flow.md` -> `Integration tests (/tests/integrationtest)`.

## Workflow

1. Use `docs/agents/testing-flow.md` -> `Integration tests (/tests/integrationtest)` for the recording command.
2. Derive `TestName` from the path under `tests/integrationtest/t/` without the `.test` suffix (example: `planner/core/binary_plan`).
3. Review changed files in `tests/integrationtest/r/**` and keep result diffs minimal.

## Guardrails

- Prefer targeted recording of the affected suite only.
- Avoid unrelated result churn.
- If result files need manual edits, keep them minimal and verify with another run.

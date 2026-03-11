---
name: tidb-integrationtest-recorder
description: Use when recording TiDB integration tests under tests/integrationtest and verifying regenerated result files stay minimal and correct.
---

# TiDB Integration Test Recorder

## Overview

Use this workflow for changes under `tests/integrationtest/t/**` or when SQL behavior needs integration coverage.
Do not use `-record` for this suite.
Canonical command details live in `docs/agents/testing-flow.md` -> `Integration tests (/tests/integrationtest)`.

## Command

```bash
pushd tests/integrationtest
./run-tests.sh -r <TestName>
popd
```

## Mapping and Review

- `TestName` maps to the test file path without extension.
- Example: `tests/integrationtest/t/planner/core/binary_plan.test` -> `planner/core/binary_plan`.
- Review changed files in `tests/integrationtest/r/**`.
- Keep result diffs minimal and confirm each line matches expected behavior.

## Guardrails

- Prefer targeted recording of the affected suite only.
- Avoid unrelated result churn.
- If result files need manual edits, keep them minimal and verify with another run.

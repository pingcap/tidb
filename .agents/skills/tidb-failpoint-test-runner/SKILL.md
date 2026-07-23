---
name: tidb-failpoint-test-runner
description: Use when running TiDB package tests and deciding whether failpoint enable/disable is required before and after the test command.
---

# TiDB Failpoint Test Runner

## Overview

Follow this workflow before running package tests under `pkg/...`.
`-tags=intest,deadlock` does not enable failpoints.
Canonical command details live in `docs/agents/testing-flow.md` -> `Failpoint decision for unit tests`.

## Workflow

1. Use `docs/agents/testing-flow.md` -> `Failpoint decision for unit tests` to decide whether the package needs failpoint enablement.
2. Run the matching command set from `docs/agents/testing-flow.md`:
   - `Failpoint-enabled run` when the package matches the failpoint checks.
   - `Unit tests (/pkg/...)` when it does not.
3. Keep the run targeted with `-run <TestName>`; for Bazel-specific variants, see the Bazel notes in `docs/agents/testing-flow.md` -> `Failpoint-enabled run`.
4. Record the decision evidence and exact test command in the final report.

---
name: tidb-failpoint-test-runner
description: Use when running TiDB package tests and deciding whether failpoint enable/disable is required before and after the test command.
---

# TiDB Failpoint Test Runner

## Overview

Follow this workflow before running package tests under `pkg/...`.
`-tags=intest,deadlock` does not enable failpoints.
Canonical command details live in `docs/agents/testing-flow.md` -> `Failpoint decision for unit tests`.

## Workflow Checklist

1. Run failpoint decision commands from `docs/agents/testing-flow.md` -> `Failpoint decision for unit tests`.
2. If any decision command matches, run the cleanup-safe failpoint wrapper from the same section (`Failpoint-Enabled Run` block).
3. If no decision command matches, run the targeted no-failpoint command from `docs/agents/testing-flow.md` -> `Unit tests (/pkg/...)`.
4. Keep runs targeted with `-run <TestName>`.
5. For Bazel-specific failpoint flow and full command variants, follow `docs/agents/testing-flow.md`.
6. Record the decision evidence and exact test command in the final report.

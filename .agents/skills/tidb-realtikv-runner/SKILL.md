---
name: tidb-realtikv-runner
description: Use when running tests under tests/realtikvtest that require a local TiUP playground lifecycle with strict startup, readiness checks, and cleanup.
---

# TiDB RealTiKV Runner

## Overview

Use this skill for test targets under `tests/realtikvtest/**`.
Always start playground in the background, verify readiness, run scoped tests, then clean up process and data.
Canonical command details live in `docs/agents/testing-flow.md` -> `RealTiKV tests (/tests/realtikvtest)`.

## Workflow Checklist

1. Choose the standard flow or `Cleanup-safe template` from `docs/agents/testing-flow.md` -> `RealTiKV tests (/tests/realtikvtest)`.
2. Use default `PD_ADDR=127.0.0.1:2379` unless a port conflict requires `--pd.port` or `--port-offset`.
3. Keep the test scope narrow (`-run <TestName>` and target subdir only).
4. If failpoints are needed, enable before tests and disable afterward.
5. Verify teardown by confirming the PD endpoint is unreachable after cleanup.

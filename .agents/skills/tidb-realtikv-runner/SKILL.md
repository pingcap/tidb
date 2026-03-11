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

1. Choose command set from `docs/agents/testing-flow.md`:
   - `RealTiKV tests (/tests/realtikvtest)` for standard flow
   - `Cleanup-safe template` for interruption-safe local runs
2. Use default `PD_ADDR=127.0.0.1:2379` unless port conflict requires `--pd.port` or `--port-offset`.
3. Keep test scope narrow (`-run <TestName>` and target subdir only).
4. If failpoints are needed, enable before tests and disable afterward.
5. Verify teardown by confirming PD endpoint is unreachable after cleanup.

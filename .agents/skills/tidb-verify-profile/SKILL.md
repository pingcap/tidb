---
name: tidb-verify-profile
description: Use when choosing local validation scope in TiDB work, especially to separate fast coding-loop checks from completion checks and avoid unnecessary slow commands.
---

# TiDB Verification Profiles

## Overview

Use this skill to decide how much local validation to run before and after code changes.
Policy requirements still come from `AGENTS.md`; this skill is the execution guide.

## Profiles

### `WIP` (coding loop)

Use while still iterating and not claiming the task is complete.

- Run only the smallest scoped checks that validate the changed behavior.
- Prefer targeted unit tests (`go test -run <TestName> -tags=intest,deadlock`).
- Avoid slow sweeps by default (`make lint`, package-wide runs, `realtikvtest`).

### `Ready` (completion gate)

Use when claiming task completion or PR readiness.
Mandatory trigger phrases are defined in `AGENTS.md` -> `Quick Decision Matrix`.

1. Map changed paths to required test surfaces via `AGENTS.md` -> `Task -> Validation Matrix`.
2. Run minimum required targeted tests for those surfaces.
3. If code changed, run `make lint`.
4. Follow `AGENTS.md` -> `Agent Output Contract` for final reporting.

### `Heavy` (explicitly required)

Use only when scope or user request requires expensive checks.

- Examples: CI reproduction, broad refactor confidence, change scope requiring RealTiKV.
- Never run `make bazel_lint_changed` unless the user explicitly requests it.

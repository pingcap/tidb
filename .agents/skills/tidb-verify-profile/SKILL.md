---
name: tidb-verify-profile
description: Choose validation for TiDB changes during iteration and delivery, using the checks required for each change type.
---

# TiDB Verification Profiles

## Overview

Use this skill to select validation for repository changes, including code, formatting, documentation, testdata, and build configuration. Read-only analysis does not require build/test checks.
Policy requirements still come from `AGENTS.md`; this skill is the execution guide.

## Profiles

### `WIP` (coding loop)

Use while iterating on a change.

- Run only the smallest scoped checks that validate the changed behavior.
- Prefer targeted unit tests (`go test -run <TestName> -tags=intest,deadlock`).
- Avoid slow sweeps by default (`make lint`, package-wide runs, `realtikvtest`).

### `Ready` (completion gate)

Use when delivering changes or preparing a PR, as defined in `AGENTS.md` -> `Quick Decision Matrix`. Select checks from the actual change type; status wording neither adds nor waives checks.

1. Map changed paths and change types to `AGENTS.md` -> `Task -> Validation Matrix` and the applicable special cases in `Quick Decision Matrix`.
2. Run the required checks for those changes. Preserve regression evidence for bug fixes, documentation review for agent instructions/skills, and scoped checks for testdata or build configuration. Formatting-only changes do not require RealTiKV tests.
3. If code changed, run `make lint`.
4. Follow `AGENTS.md` -> `Agent Output Contract` for final reporting.

Reuse completed checks that still cover the delivered changes. Rerun affected checks when relevant changes or new failures invalidate the results, not merely because another status update is due.

### `Heavy` (explicitly required)

Use only when scope or user request requires expensive checks.

- Examples: CI reproduction, broad refactor confidence, change scope requiring RealTiKV.
- Never run `make bazel_lint_changed` unless the user explicitly requests it.

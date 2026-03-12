---
name: tidb-bazel-prepare-gate
description: Use when deciding whether make bazel_prepare is required before build or test commands based on local file changes in TiDB.
---

# TiDB Bazel Prepare Gate

## Overview

Use this skill before build/test commands when you are unsure whether `make bazel_prepare` is required.
Policy source: `AGENTS.md` -> `Build Flow` -> `When make bazel_prepare is required`.
In normal coding loops, skip this skill unless one of the decision-rule triggers is likely present.

## Inspect Local Changes

Run from repository root:

```bash
git status --short
git diff --name-status
git diff --name-status --cached
git ls-files --others --exclude-standard
```

## Decision Rules

Run `make bazel_prepare` if any of these are true:

- New workspace or fresh clone.
- Changed Bazel files (for example `WORKSPACE`, `DEPS.bzl`, `BUILD.bazel`, `MODULE.bazel`, `MODULE.bazel.lock`).
- Any Go source file is added/removed/renamed/moved.
- `go.mod` or `go.sum` changed.
- UT or RealTiKV tests were added and Bazel test targets changed.
- Local Bazel dependency/toolchain errors occurred.

If none of the rules match, continue without `make bazel_prepare` and report the evidence.

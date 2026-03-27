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
git diff -U0 -- '*_test.go'
git diff -U0 --cached -- '*_test.go'
```

## Decision Rules

Trigger conditions are defined in `AGENTS.md` -> `Build Flow` -> `When make bazel_prepare is required`.
Compare the output from the commands above against those conditions.

For the top-level test-function trigger in existing `*_test.go` files, inspect added lines in the
`git diff -U0` output for patterns like:

```diff
+func TestXxx(t *testing.T) {
```

and treat that as requiring `make bazel_prepare`.

If any condition matches, run `make bazel_prepare`.
If none of the rules match, continue without `make bazel_prepare` and report the evidence.

---
name: tidb-bazel-prepare-gate
description: Assess TiDB Bazel metadata impact and choose no generation, CI generation, or local preparation without blocking ordinary Go tests.
---

# TiDB Bazel Metadata Gate

Policy source: `AGENTS.md` -> `Build Flow` -> `Bazel metadata consistency`.
Read `docs/agents/metadata-generation-flow.md` before selecting or executing a generation route.

## Inspect the intended PR

Identify the actual target branch and merge base. Inspect committed, staged, unstaged, and intended untracked changes. Exclude unrelated user probes.

```bash
git status --short
git diff --name-status <merge-base> HEAD
git diff -U0 <merge-base> HEAD -- '*.go'
git diff --name-status
git diff --name-status --cached
git ls-files --others --exclude-standard
git diff -U0 -- '*.go'
git diff -U0 --cached -- '*.go'
```

Use the complete input list in root policy, not just Go diffs. Read affected declarations to assess imports, test counts, build constraints, embedded resources, and directives; regex matches alone are not a semantic classifier.

## Select a route

- No relevant input change: skip local preparation and run scoped Go validation.
- Metadata-affecting changes with applicable CI generation: run Go validation first, publish with metadata pending, then incorporate verified current-head generated changes using the runbook.
- No applicable CI generation: use target-branch canonical local preparation before delivery.
- Dependency, rule, toolchain, or generator changes: arrange applicable Bazel validation in addition to generation.

Do not distort test structure merely to avoid generation. Adding a top-level test is fine; select the appropriate route. A fresh worktree or branch switch alone is not a generation trigger. Diagnose Bazel errors separately from metadata drift.

Report the selected route, input evidence, and whether metadata verification is complete or pending.

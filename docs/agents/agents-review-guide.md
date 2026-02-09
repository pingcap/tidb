# AGENTS.md Review Guide

This document is a repeatable review guide for changes to `AGENTS.md` and related agent runbooks.
Normative requirements (`MUST`/`SHOULD`) remain in root `AGENTS.md`.

## When to Use

Run this guide before merging changes to:

- `AGENTS.md`
- Any added/updated/removed agent-facing docs under `docs/agents/`

## Document Boundaries (Source of Truth)

- `AGENTS.md`: policy and contract (normative behavior).
- `docs/agents/`: supporting runbooks, indexes, and review checklists (for example architecture index and testing flow).

Review gate:

- [ ] Any new `MUST`/`SHOULD` policy lives in `AGENTS.md` first.
- [ ] Supporting docs only explain or exemplify policy; they do not introduce new policy scope.

## Review Workflow

### 1) Precedence and Scope

- [ ] Confirm precedence wording remains clear (`MUST`/`SHOULD`/`MAY`, root-first precedence).
- [ ] If subtree-specific guidance is added, ensure it does not conflict with root `AGENTS.md`.

### 2) Structure and Duplication

- [ ] `AGENTS.md` remains concise: policy-focused, not overloaded with step-by-step runbook detail.
- [ ] Detailed workflows are linked from `AGENTS.md` to `docs/agents/*` instead of copied inline.
- [ ] No duplicated checklist documents with overlapping normative rules.

### 3) High-Risk Policy Gates

Validate these first because they caused prior drift/regressions:

- [ ] Bazel metadata rule is explicit and unambiguous (no ambiguous wildcard wording).
- [ ] PR requirements include the `Issue Number:` line with `close #<id>` or `ref #<id>`.
- [ ] Notes update policy is consistent between `AGENTS.md` and planner notes.
- [ ] Testing policy in `AGENTS.md` matches testing runbook guidance under `docs/agents/` (no contradiction).

### 4) Testing and Validation Consistency

- [ ] `Task -> Validation Matrix` still defines minimal required test surfaces by change scope.
- [ ] `Testing Policy` remains policy-level and points to command playbooks under `docs/agents/`.
- [ ] Integration recording rule remains `./run-tests.sh -r <TestName>`.
- [ ] RealTiKV rule still requires background start and mandatory cleanup.
- [ ] Bug-fix policy still requires regression tests with fail-before-fix/pass-after-fix evidence (or explicit infeasibility note).

### 5) PR/Issue Policy Consistency

- [ ] PR title format rules are intact.
- [ ] PR description still requires `.github/pull_request_template.md`.
- [ ] HTML comment preservation requirement remains intact.
- [ ] English language requirement remains intact for issues and PRs.

### 6) Reference and Path Hygiene

- [ ] Every mentioned path exists.
- [ ] Removed files are not referenced anywhere.
- [ ] Cross-links between `AGENTS.md` and docs under `docs/agents/` remain valid.
- [ ] Shell snippets are copy-paste safe (especially quoted patterns containing backticks).
- [ ] Any `make <target>` mention in docs is verified against `Makefile`.
- [ ] Related examples stay parameter-consistent across snippets.

## Fast Verification Commands

Use from repository root.

```bash
# Check critical policy anchors in AGENTS.md
grep -n "Issue Number:" AGENTS.md
grep -n "Task -> Validation Matrix\|Testing Policy\|make bazel_prepare" AGENTS.md

# Check cross-doc source-of-truth boundaries in docs/agents/
grep -R -n 'root `AGENTS.md`' --include="*.md" docs/agents

# Ensure removed docs are not referenced
grep -R -n "pr-issue.md" --include="*.md" . --exclude="agents-review-guide.md"
```

## Acceptance Criteria for AGENTS-related Changes

A review is complete only when all are true:

- [ ] No policy contradiction across `AGENTS.md` and the changed docs under `docs/agents/`.
- [ ] No ambiguous wording for critical rules (especially test/build and PR gates).
- [ ] No stale path references.
- [ ] `AGENTS.md` preserves policy-level clarity and does not re-accumulate large runbook detail.
- [ ] Final review comment includes concrete evidence (paths and exact commands run).

## Reviewer Output Template

Use this summary format in review comments:

1. Files reviewed.
2. Drift/contradiction findings.
3. Commands run to verify references/rules.
4. Required fixes before merge.
5. Optional follow-ups (non-blocking).

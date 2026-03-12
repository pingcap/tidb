---
name: tidb-issue-metadata-guard
description: Use when creating or editing TiDB GitHub issues so issue templates, labels, issue titles, and issue descriptions stay consistent with repository workflow. Trigger on tasks involving issue creation, bug reports, enhancement tracking issues, label selection, or searching for existing issues and PRs before filing a new one.
---

# TiDB Issue Metadata Guard

## Overview

Use this skill for TiDB GitHub issue metadata updates.
The goal is to preserve issue-template structure, label hygiene, and searchable issue descriptions.

Before creating an issue, read the matching file under `.github/ISSUE_TEMPLATE/`.

## Workflow

1. Write issue titles and descriptions in English.
2. Search existing issues and PRs first when the task is bug reporting or tracking an existing change.
3. Start from the matching issue template instead of writing the body from scratch.
   - Follow the template and fill the required sections.
   - For bug reports, include minimal reproduction, expected behavior, actual behavior, and TiDB version information when available.
   - Fill issue fields with concrete values from the available artifacts instead of generic placeholders.
   - Keep the first screen readable: show the top-level repro summary, expected result, actual result, and version directly in the section body.
   - If a field is long, keep a short visible summary and put the full content in a `<details>` block.
     - Good candidates: full repro SQL, full schema, long logs, checksum diffs, execution plans, and plan replayer notes.
     - Do not hide the only reproduction steps or the top-level expected/actual conclusion inside `<details>`.
   - If supporting artifacts exist, mention them explicitly in the repro steps or attachments summary.
   - When adding an analysis section, keep it short and evidence-backed:
     - 1–3 likely causes at most
     - prefer evidence from SQL shape, plans, errors, or result diffs
4. If you create an issue with `gh issue create`, add labels explicitly when the GitHub UI would normally auto-apply them.
   - Add at least one `component/*` label.
   - For bug or regression issues, add `severity/*` and affected-version labels when appropriate.
   - Severity labeling rule:
     - Wrong-result bugs: use `severity/major`.
     - Query fails to execute on valid SQL, for example an internal planner or runtime error: use `severity/major`.
     - Complex-query or compatibility issues that are not confirmed wrong-result and are not execution-blocking: use `severity/moderate`.
   - If label permissions are missing, first add labels by commenting `/label <label name>`.
   - If label comments still do not work, add a separate comment with `Suggested labels: ...`.
5. Prefer file-based edits for GitHub metadata.
   - Materialize the intended issue body in a local Markdown file.
   - Review that file against the matching issue template before calling `gh`.

## Quick Checks

- The issue title and body are in English.
- The issue still follows the matching template structure.
- Long raw artifacts are folded with `<details>` when needed, while the top-level summary remains visible.
- The issue carries the expected labels, or a follow-up label comment / `Suggested labels: ...` comment is present when label permissions are missing.

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
4. If you create an issue with `gh issue create`, add labels explicitly when the GitHub UI would normally auto-apply them.
   - Add at least one `component/*` label.
   - For bug or regression issues, add `severity/*` and affected-version labels when appropriate.
   - If label permissions are missing, include `Suggested labels: ...` in the issue body.
5. Prefer file-based edits for GitHub metadata.
   - Materialize the intended issue body in a local Markdown file.
   - Review that file against the matching issue template before calling `gh`.

## Quick Checks

- The issue title and body are in English.
- The issue still follows the matching template structure.
- The issue carries the expected labels, or the body includes `Suggested labels: ...` when label permissions are missing.

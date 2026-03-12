---
name: tidb-pr-metadata-guard
description: Use when creating or editing TiDB pull requests so PR title scope, PR template fields, hidden HTML comments, and bot-parsed checklist sections stay intact. Trigger on tasks involving PR creation, PR body updates, issue linking from a PR, test checklist updates, or investigating labels like do-not-merge/needs-tests-checked.
---

# TiDB PR Metadata Guard

## Overview

Use this skill for TiDB GitHub pull request metadata updates.
The goal is to preserve repository-required PR structure while editing only the mutable fields.

Before changing a PR body, read `.github/pull_request_template.md`.

## Workflow

1. Write PR titles and descriptions in English.
2. For a new PR, start from `.github/pull_request_template.md` instead of writing the body from scratch.
   - Use a title in the form `pkg [, pkg2, pkg3]: what is changed` or `*: what is changed`.
   - In that title format, `pkg` means the TiDB module area, not a literal Go package path. For example, changes under `pkg/planner/core` usually map to `planner`, not `pkg/planner/core`.
   - Use `gh pr create -T .github/pull_request_template.md`.
   - Fill in the template in a Markdown file first, then submit it.
3. For an existing PR, update only the mutable sections.
   - Safe targets: `Issue Number:`, `Problem Summary:`, the content under `### What changed and how does it work?`, test checkbox states and concrete commands, and the `release-note` block.
   - Do not rename headings, reorder checklist sections, or rewrite the template wholesale.
4. Preserve hidden HTML comments exactly.
   - Keep `Tests <!-- At least one of them must be included. -->` unchanged.
   - Keep the `No need to test` nested block and its HTML comment unchanged.
   - Do not delete or rewrite template comments that explain issue linking or release-note behavior.
5. If a PR needs a new linked issue, use `tidb-issue-metadata-guard` to create or identify the issue first, then patch only the `Issue Number:` line in the PR body.
6. Prefer file-based edits for GitHub metadata.
   - Materialize the intended issue body or PR body into a local Markdown file.
   - Review that file against the PR template before calling `gh`.
7. After any PR body update, re-read the PR and check whether bot-gated labels changed as expected.
   - `do-not-merge/needs-linked-issue`
   - `do-not-merge/needs-tests-checked`
   - If a label remains unexpectedly, diff the current body against `.github/pull_request_template.md` before changing anything else.

## Quick Checks

- The PR body still contains an `Issue Number:` line, and that line can reference one or more related issues with full keyword syntax such as `close #<id>` and `ref #<id>`.
- The PR title uses module-oriented scope names such as `planner`, `executor`, or `*:`, not raw Go package paths such as `pkg/planner/core`.
- The `Tests` line still includes the template HTML comment verbatim.
- At least one test checkbox is checked, or `No need to test` is checked with a reason.

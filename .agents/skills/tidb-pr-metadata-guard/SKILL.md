---
name: tidb-pr-metadata-guard
description: Use when creating or editing TiDB pull requests so PR template fields, hidden HTML comments, and bot-parsed checklist sections stay intact. Trigger on tasks involving PR body updates, issue linking, test checklist updates, or investigating labels like do-not-merge/needs-tests-checked.
---

# TiDB PR Metadata Guard

## Overview

Use this skill for TiDB GitHub metadata updates that touch PR descriptions or linked issues.
The goal is to preserve repository-required template structure while editing only the mutable fields.

Before changing a PR body, read `.github/pull_request_template.md`.

## Workflow

1. For a new PR, start from `.github/pull_request_template.md` instead of writing the body from scratch.
   - Use `gh pr create -T .github/pull_request_template.md`.
   - Fill in the template in a Markdown file first, then submit it.
2. For an existing PR, update only the mutable sections.
   - Safe targets: `Issue Number:`, `Problem Summary:`, the content under `### What changed and how does it work?`, test checkbox states and concrete commands, and the `release-note` block.
   - Do not rename headings, reorder checklist sections, or rewrite the template wholesale.
3. Preserve hidden HTML comments exactly.
   - Keep `Tests <!-- At least one of them must be included. -->` unchanged.
   - Keep the `No need to test` nested block and its HTML comment unchanged.
   - Do not delete or rewrite template comments that explain issue linking or release-note behavior.
4. If a PR needs a new linked issue, create or identify the issue first, then patch only the `Issue Number:` line in the PR body.
5. Prefer file-based edits for GitHub metadata.
   - Materialize the intended issue body or PR body into a local Markdown file.
   - Review that file against the repository template before calling `gh`.
6. After any PR body update, re-read the PR and check whether bot-gated labels changed as expected.
   - `do-not-merge/needs-linked-issue`
   - `do-not-merge/needs-tests-checked`
   - If a label remains unexpectedly, diff the current body against `.github/pull_request_template.md` before changing anything else.

## Quick Checks

- The PR body still contains one `Issue Number:` line with `close #<id>` or `ref #<id>`.
- The `Tests` line still includes the template HTML comment verbatim.
- At least one test checkbox is checked, or `No need to test` is checked with a reason.
- If you created an issue via `gh issue create`, read the matching template in `.github/ISSUE_TEMPLATE/` and add labels explicitly when the UI would normally auto-apply them.

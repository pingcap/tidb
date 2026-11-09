# TiDB Pull Request Guide

This document is the PR-specific guide linked from root `AGENTS.md`.
Use root `AGENTS.md` as the entry point, then read this file for PR preparation and review handoff details.

## When to Read

Use this guide when you need detail for:

- pull request preparation and review handoff

## PR Workflow

- PR titles should use one of:
  - `pkg [, pkg2, pkg3]: what is changed`
  - `*: what is changed`
- PR descriptions should follow `.github/pull_request_template.md`.

- Keep required HTML comments unchanged, including `Tests <!-- At least one of them must be included. -->`, because CI tooling depends on them.
- Fill the template sections completely:
  - use the `commit-message` code block for the detailed commit message body
  - mark the relevant test and side-effect checklist items
  - include a release note in the `release-note` code block, or `None` if not applicable
- Avoid force-push when possible; prefer follow-up commits and squash merge.
- If force-push is unavoidable, use `--force-with-lease` and coordinate with reviewers.

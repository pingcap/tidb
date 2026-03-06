---
name: github-workflow
description: Guidelines for creating GitHub issues and PRs in the TiDB repository. Use when creating, reviewing, or editing issues and pull requests.
---

# GitHub Issue and PR Workflow

## Issue Rules

- Follow templates under `.github/ISSUE_TEMPLATE/` and fill all required fields.
- Bug reports should include minimal reproduction, expected/actual behavior, and TiDB version (for example `SELECT tidb_version()` output).
- Search existing issues/PRs first (for example `gh search issues --repo pingcap/tidb --include-prs "<keywords>"`), then add relevant logs/configuration/SQL plans.
- Labeling requirements:
  - `type/*` is usually applied by the issue template (GitHub UI); if creating issues via `gh issue create`, add it explicitly via `--label` (or follow up with `gh issue edit --add-label`).
  - Add at least one `component/*` label.
  - For bug/regression, include `severity/*` and affected-version labels (for example `affects-8.5`, or `may-affects-*` if unsure).
  - If label permissions are missing, include `Suggested labels: ...` in issue body.

## PR Requirements

- PR title MUST use one of:
  - `pkg [, pkg2, pkg3]: what is changed`
  - `*: what is changed`
- PR description MUST follow `.github/pull_request_template.md`.
- PR description MUST contain one line starting with `Issue Number:` and reference related issue(s) using `close #<id>` or `ref #<id>`.
- If you create PRs via GitHub CLI, start from the template to avoid breaking required HTML comments: `gh pr create -T .github/pull_request_template.md` (then fill in the fields; do not delete/alter the HTML comment markers).
- Keep HTML comments unchanged, including `Tests <!-- At least one of them must be included. -->`, because CI tooling depends on them.
- Avoid force-push when possible; prefer follow-up commits and squash merge.
- If force-push is unavoidable, use `--force-with-lease` and coordinate with reviewers.

### Writing Good PR Descriptions

- **"Problem Summary" (why) is the primary section.** Lead with why the change is needed: what problem exists, what limitation it removes, or what invariant it restores. The reviewer should understand the motivation before reading any code.
- **"What changed" (how) is secondary.** Lead with a 1-3 sentence summary of the general approach, then optionally follow with a bullet list of notable specifics. The summary should explain the approach and trade-offs; the list provides scannable detail for reviewers. Do not skip the summary and jump straight to a list of mechanical changes.
- Bad example (list without motivation): "Rename function A to B. Remove function C. Add parameter X to function D. Update callers E, F, G." -- this restates the diff without explaining why.
- Good example (summary + details): "Callers of A need both X and Y, but A only returns X, so every caller has to compute Y separately with duplicated logic. Combine both into A so callers get everything in one call. Specifically: - Merged computeX and computeY into A. - Updated callers in pkg/foo and pkg/bar to use the new signature." -- this explains the problem first, then lists notable changes for scannability.

### Writing Good Issue Reports

- Lead with what you observed vs. what you expected, not with internal implementation details.
- Include a minimal reproduction: SQL statements, configuration, and version.
- Attach relevant logs, `EXPLAIN` output, or stack traces in collapsible sections.
- If you suspect a root cause, mention it after the reproduction, not instead of it.

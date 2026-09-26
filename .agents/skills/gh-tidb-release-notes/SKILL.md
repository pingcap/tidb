---
name: gh-tidb-release-notes
description: Generate end-user-facing release note lines for TiDB pull requests by reading the PR and linked issue with GitHub CLI. Use when asked to summarize one or more TiDB PRs, cherry-picks, or issues into release notes, especially when deciding whether a change should become a one-line note or `None(reason)`.
---

# GH TiDB Release Notes

Turn TiDB PRs into short release-note lines that describe the user-visible behavior change. Use `gh` to inspect the PR, its linked issue, and the original upstream PR when a cherry-pick PR is too thin.

## Workflow

1. Read each PR with `gh api` or `gh pr view`.
2. Extract the PR title, body, issue references, release-note block, and whether it is a cherry-pick.
3. Read the linked issue to understand the actual symptom, expected behavior, and affected scenario.
4. If the PR is a cherry-pick and the backport PR is sparse, read the original upstream PR too.
5. Reduce the change to the end-user-visible behavior, then write one sentence or return `None(reason)`.

## Decision Rules

- Write a release note only when the change is visible to TiDB users or operators.
- Treat these as user-visible:
  - bug fixes that change query behavior, upgrade behavior, privilege checks, or compatibility
  - new features or new supported SQL behavior
  - meaningful performance improvements that users can observe in common operations
  - changes that help external tools or operators interact with TiDB
- Return `None(reason)` for changes that are not user-visible, such as:
  - flaky test fixes
  - added debug logs
  - pure refactors or internal data-structure changes
  - test-only changes
- If a PR is mostly internal but the outcome is user-visible, describe the outcome and omit the implementation.
- For cherry-picks, ignore the cherry-pick mechanics and describe the actual behavior change.

## Style Rules

- Keep each release note to one sentence.
- For bug fixes, prefer `Fix the issue that ...`
- Focus on behavior and outcome, not code-level concepts.
- Avoid internal names such as function names, cache names, planner internals, or bootstrap step names unless the object is directly user-visible, such as `mysql.tidb`.
- Quote SQL syntax or SQL statements with backticks, for example `KILL QUERY`, `JOIN ... USING`, or `INSERT ... ON DUPLICATE KEY UPDATE`.
- Do not mention PR numbers, commit hashes, file paths, tests, or reviewers in the release-note line.
- Prefer concrete symptoms such as "can fail during upgrade", "returns incomplete results", or "checks privileges incorrectly".

## Wording Patterns

- Bug fix:
  - `Fix the issue that ...`
- New feature or support:
  - `Add ...`
  - `Support ...`
- Improvement:
  - `Improve ...`
- No release note:
  - `None(fix flaky test)`
  - `None(add debug logs)`
  - `None(internal refactor)`

## Output Shape

Use one line per PR:

`<identifier>: <release-note line>`

If the caller does not want identifiers, output only the release-note lines.

## Examples

- `Fix the issue that \`KILL QUERY\` incorrectly kills idle connections.`
- `Fix the issue that after \`EXCHANGE PARTITION\`, non-unique global indexes on non-clustered partitioned tables can return incomplete results.`
- `Support column-level privileges in \`GRANT\` and \`REVOKE\`.`
- `None(add debug logs)`

## Notes

- Prefer the linked issue over the PR title when the title is vague.
- Prefer the original upstream PR over the backport PR when the backport body omits details.
- If the PR only adds logs or tests, return `None(...)` even if the PR title sounds important.

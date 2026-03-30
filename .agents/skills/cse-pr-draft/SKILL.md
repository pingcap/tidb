---
name: cse-pr-draft
description: Generate a TiDB CSE pull request title and body draft from the current branch diff, recent commits, and the repository PR template. Use when asked to prepare a PR description, draft a PR, or create title/body text before opening a PR.
---

# TiDB CSE PR Draft

Use this skill to produce a ready-to-paste PR title and PR body draft before running `gh pr create`.

This skill drafts the content.
Use `cse-pr-create` to validate and create the actual PR.

## Goal

Produce two outputs:

1. A PR title that matches the repository format:
   - `pkg [, pkg2, pkg3]: what's changed`
   - `*: what's changed`
2. A PR body draft that preserves the structure of `.github/pull_request_template.md`

## Required inputs

Before drafting, inspect all of the following:

1. Current branch name
2. Git status
3. Full diff to be included in the PR
4. Recent commits on the branch
5. `.github/pull_request_template.md`
6. Real linked issue number in `tidbcloud/tidb-cse`, or enough context to create one

Do not draft a final PR body with `Issue Number: close #xxx` or `Issue Number: ref #xxx`.
Do not draft a final PR body that links an upstream issue from another repository.

If no local issue exists yet, create one automatically before returning the final PR draft.

## Drafting workflow

### 1. Collect branch context

Use commands equivalent to:

```bash
git branch --show-current
git status --short
git log --oneline --decorate -n 10
git diff --stat
git diff
```

If the branch is meant for a PR against a base branch, also inspect the branch-only history:

```bash
git log --oneline <base-branch>..HEAD
git diff <base-branch>...HEAD
```

### 2. Infer affected package scope

Choose the title prefix from the main packages touched by the diff.

Examples:

- only `br/pkg/lightning/...` changed → `br/lightning: ...`
- only `pkg/executor/...` changed → `pkg/executor: ...`
- related changes across two packages → `pkg/executor, pkg/lightning: ...`
- broad infra or repo-wide changes → `*: ...`

Keep the scope short and recognizable from repository paths.

### 3. Write the title

The title should describe the real change, not just the files touched.

Good examples:

- `br/lightning: add row number to encode kv errors`
- `pkg/executor: improve import encode error context`
- `*: document repo-local PR workflow for agents`

Bad examples:

- `fix bug`
- `update code`
- `changes`

### 4. Write the PR body draft

Keep the exact template structure and fill it with concise, review-friendly content.

Draft the body as a fully ready final body. The create skill may need to use a two-step `gh pr create -T ... --fill` followed by a REST patch because `gh pr create` cannot combine `--template` with `--body`/`--body-file` in non-interactive mode.

Required sections to fill:

- `### What problem does this PR solve?`
- `Issue Number: close #<id>` or `Issue Number: ref #<id>`
- `Problem Summary:`
- `### What changed and how does it work?`
- `### Check List`
- `Tests`
- `Side effects`
- `Documentation`
- `### Release note`

### 4.1 Automatic issue creation

If the caller does not provide a local issue number, draft and create the issue first.

Use `gh issue create --repo tidbcloud/tidb-cse` with:

- a concise issue title based on the branch diff
- a short problem statement
- expected behavior or desired workflow
- scope / planned change summary

For multi-line issue body, pass it via HEREDOC to avoid literal `\\n` text in GitHub:

```bash
gh issue create --repo tidbcloud/tidb-cse --title "<issue title>" --body "$(cat <<'EOF'
<issue body>
EOF
)"
```

Then use the created local issue number in the PR draft:

````text
Issue Number: close #<id>
Issue Number: ref #<id>
````

## Writing rules

- Write in English.
- Keep the summary focused on reviewer value: problem, approach, validation.
- Use 2-5 bullets in `What changed and how does it work?` when helpful.
- The `Tests` section must reflect real validation already run or planned.
- If no local `tidbcloud/tidb-cse` issue number is available, create one before returning the final PR draft.
- Do not remove checklist items from the template.
- Do not invent benchmark or test results.
- Do not link `pingcap/tidb` or any other repository in the `Issue Number:` line.
- Assume the create step may need to patch the PR body via `gh api`; therefore the draft must already be complete and ready to send as one final string.

## Recommended output format

When returning a draft, use this exact structure:

````text
Title: <final PR title>

Body:
### What problem does this PR solve?
Issue Number: close #123

or

Issue Number: ref #123

Problem Summary:
<1-3 sentences>

### What changed and how does it work?
- <bullet>
- <bullet>

### Check List

Tests <!-- At least one of them must be included. -->

- [x] Unit test
- [ ] Integration test
- [ ] Manual test (add detailed scripts or steps below)
- [ ] No need to test
  > - [ ] I checked and no code files have been changed.

Side effects

- [ ] Performance regression: Consumes more CPU
- [ ] Performance regression: Consumes more Memory
- [ ] Breaking backward compatibility

Documentation

- [ ] Affects user behaviors
- [ ] Contains syntax changes
- [ ] Contains variable changes
- [ ] Contains experimental features
- [ ] Changes MySQL compatibility

### Release note

```release-note
None
```
````

## Handoff to PR creation

After drafting the content:

1. Re-check title format
2. Re-check the `Issue Number:` line uses a real local `tidbcloud/tidb-cse` issue ID
3. Hand off a fully final body string to `cse-pr-create`; do not assume the create step can safely rebuild the body later
4. If the branch lives in the same repository, avoid `owner:branch` head syntax during creation unless the head is actually in a fork
5. If GitHub CLI GraphQL commands fail with classic-project `projectCards` errors during the create/verify step, use `gh api` REST calls instead

## References

- `.github/pull_request_template.md`
- `.agents/skills/cse-pr-create/SKILL.md`

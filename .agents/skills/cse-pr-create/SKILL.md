---
name: cse-pr-create
description: Create a TiDB CSE pull request that satisfies repository title and linked-issue requirements. Use when opening or preparing a PR in this repository, especially to avoid do-not-merge/invalid-title and do-not-merge/needs-linked-issue labels.
---

# TiDB CSE PR Creation

Use this workflow whenever you create a pull request for this repository.

## Non-negotiable requirements

### 1. PR title format

The title MUST use one of these formats from the repository template:

- `pkg: what's changed`
- `pkg1, pkg2: what's changed`
- `pkg [, pkg2, pkg3]: what's changed` as the general pattern
- `*: what's changed`

Examples:

- `br/lightning: add row number to encode kv errors`
- `pkg/executor, pkg/lightning: improve import error context`
- `*: update CI metadata`

If the title does not match this pattern, GitHub adds `do-not-merge/invalid-title`.

### 2. Linked issue line in PR body

Before creating a PR, create or identify the linked issue in `github.com/tidbcloud/tidb-cse`.

If no local issue exists yet, create one automatically before opening the PR.

The PR body MUST contain exactly one line starting with `Issue Number:` and link that local issue with `close` or `ref`.

Do not open the PR until you have a real local issue number. Replace template placeholders such as `Issue Number: close #xxx` or `Issue Number: ref #xxx` with an actual issue reference from `tidbcloud/tidb-cse`.

### 2.1 Automatic issue creation

When the caller does not provide a local issue number, create one with `gh issue create` before creating the PR.

Use the current branch diff, changed package scope, and problem summary to draft the issue title/body.

Minimum issue body content:

- problem statement
- expected behavior or desired workflow
- scope / planned change summary
- optional upstream reference if relevant

Suggested flow:

```bash
gh issue create --repo tidbcloud/tidb-cse --title "<issue title>" --body "$(cat <<'EOF'
<issue body>
EOF
)"
```

Use a HEREDOC for multi-line issue bodies. Avoid embedding literal `\n` in quoted strings.

Capture the returned issue number and use it in one of:

```text
Issue Number: close #<id>
Issue Number: ref #<id>
```

Valid examples:

- `Issue Number: close #123`
- `Issue Number: ref #456`

Invalid examples:

- `Issue Number: close #xxx`
- `Issue Number: ref #xxx`
- `Issue Number: close pingcap/tidb#63763`
- `Issue Number: ref pingcap/tidb#63763`
- missing `Issue Number:` line
- multiple `Issue Number:` lines

If this line is missing, duplicated, left as placeholder text, points to another repository, or is malformed, GitHub adds `do-not-merge/needs-linked-issue`.

### 3. Use the repo template

Always create the PR from the repository template:

```bash
gh pr create -T .github/pull_request_template.md
```

Do not delete or rewrite the template structure. Keep the existing headings, checklist items, and comment blocks intact, and fill them in.

## Required workflow

1. Draft a title that matches the required format.
2. Create or identify the linked issue in `tidbcloud/tidb-cse`.
3. If no issue exists, create it automatically with `gh issue create`.
4. Do not proceed until you have a real local linked issue number.
5. Open `.github/pull_request_template.md` and keep its structure intact.
6. Fill in these template sections without changing their headings:
   - `### What problem does this PR solve?`
   - `Issue Number: close #<id>` or `Issue Number: ref #<id>`
   - `Problem Summary:`
   - `### What is changed and how it works?`
   - `### Check List`
   - `Tests`
   - `Side effects`
   - `Documentation`
   - `### Release note`
7. Keep the PR body in English.
8. Before submitting, verify both:
   - title matches `pkg [, pkg2, pkg3]: what's changed` or `*: what's changed`
   - body contains one actual `Issue Number: close #<id>` or `Issue Number: ref #<id>` line with a real local issue ID

## Suggested CLI flow

```bash
gh pr create \
  -T .github/pull_request_template.md \
  --title "br/lightning: add row number to encode kv errors"
```

Then replace the template placeholder with a valid local issue link line such as:

```text
Issue Number: close #123
Issue Number: ref #123
```

## Pre-submit checklist

- [ ] Title follows required TiDB format
- [ ] PR body keeps the repository template structure
- [ ] linked issue exists in `tidbcloud/tidb-cse`
- [ ] `Issue Number:` line exists exactly once and uses `close` or `ref`
- [ ] `Issue Number:` line uses a real issue ID, not `#xxx`
- [ ] `Issue Number:` line points to the local repo issue, not an upstream repo issue
- [ ] Title and body are in English
- [ ] Tests section is filled in
- [ ] Release note section is filled in

## References

- `.github/pull_request_template.md`
- TiDB Development Guide: "Contribute Code"
- Linked issue guidance: https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/contribute-code.html#referring-to-an-issue

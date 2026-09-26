# Bazel Metadata Flow for Agents

Root `AGENTS.md` -> `Build Flow` -> `Bazel metadata consistency` owns policy. This runbook does not replace regression, failpoint, lint, or build/test requirements.

## Local iteration

Use scoped Go tests from `docs/agents/testing-flow.md`; use `make server` when SQL checks need a new binary. Neither requires Bazel preparation first. Avoid `make precheck` as a shortcut: it includes Bazel preparation.

Inspect the entire intended PR against its actual merge base, plus pending edits. Body-only changes can skip generation. Other inputs listed in root policy need metadata verification, but need not block Go tests.

## CI generation route

1. Inspect target-branch workflows and current availability. The current master `.github/workflows/generate-bazel-files.yml` runs on PR events targeting master and produces artifact `bazel-files` containing `bazel.patch`, retained for one day. Do not assume availability for release branches or that a job actually ran.
2. Complete applicable local checks, then publish with an explicit status such as `Local Ready checks passed; Bazel metadata generation pending for <head SHA>`. Publication is not completed CI.
3. Locate a successful generation run for the exact repository, PR, and head. Check the PR head again before applying output. Missing, expired, failed, approval-blocked, or stale runs do not prove consistency. Obtain an applicable run or use canonical local preparation; do not silently waive the gate.
4. Download into a fresh temporary directory. Review the patch against the allowlist and rejection rules in `.github/workflows/update-bazel-files.yml`: generated Bazel files only, no unrelated source, unsafe paths, mode changes, or symlinks. Inspect both sides of renames/deletions. Preserve unrelated user work.
5. An empty patch means no generated changes for that head. For a nonempty patch, check applicability, apply, review, and commit/push generated files in the authorized PR workflow. Do not hand-recreate them. Fork PRs currently require this step; the updater does not push to forks automatically.
6. Recheck generation and applicable Bazel checks on the resulting head. Report metadata status separately from overall CI: generation does not establish build/test correctness.

Read-only identification examples (replace placeholders with verified values):

```bash
gh pr view <pr-number> --repo pingcap/tidb --json headRefOid,baseRefName,headRepository,statusCheckRollup
gh run list --repo pingcap/tidb --workflow generate-bazel-files.yml --commit <head-sha>
gh run view <run-id> --repo pingcap/tidb --json headSha,event,conclusion,url
```

After verifying provenance and reviewing the downloaded patch:

```bash
git apply --stat <absolute-patch-path>
git apply --check <absolute-patch-path>
# Only after path/content review and checking for overlapping user changes:
git apply <absolute-patch-path>
git diff --check
```

Application checks are not a patch-security validator. Review patch contents and updater restrictions before writing. Never use `--unsafe-paths` or force an outdated patch onto another head.

## Canonical local route

Read the target branch's Makefile and use its `make bazel_prepare` when CI generation is unavailable or local preparation is needed. Review/include generated changes. Do not reuse another branch's generator versions or dependency configuration without proving equivalence.

Dependency/rule/toolchain/generator changes additionally need relevant Bazel build/test validation locally or in CI. Diagnose download failures and toolchain mismatches rather than repeating preparation blindly. `make bazel_lint_changed` remains excluded unless explicitly requested.

## Representative routing checks

| Change | Route |
| --- | --- |
| Function-body fix and existing subtest | Go validation; no generation input change |
| Fresh worktree without metadata-input changes | No automatic local preparation |
| Added/removed top-level test or new Go file on master | Go validation first; applicable CI generation |
| Imports, build tags, embed resources, Gazelle directives | Assess metadata; CI generation if available |
| Module, dependency patch, Bazel rule, or generator changes | Generation plus applicable Bazel validation |
| Release branch without generator workflow | Canonical local preparation before delivery |
| CI patch belongs to an older head | Reject; obtain current-head generation |

Standalone Gazelle/tazel/mirror replacements are not implemented here. Until an alternative is validated against canonical output, use canonical generation locally or in CI.

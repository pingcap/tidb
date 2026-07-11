---
name: tidb-test-diff-triage
description: Triage unexpected TiDB test diffs that seem unrelated to the current PR. Use when plan/result/testdata changes appear after merge/rebase or only in specific local runs, especially to quickly rule in/out failpoint enablement issues.
---

# TiDB Test Diff Triage

## Trigger

Use this workflow when:

- A test diff appears, but touched code in the PR does not explain that behavior change.
- A single test run and full-suite run produce different outputs.
- Planner/executor testdata changed unexpectedly after merge/rebase.

## Rule 1: rule out failpoint setup first

In TiDB, many test behaviors rely on failpoint instrumentation. `-tags=intest,deadlock` does not enable failpoints.

- Use `docs/agents/testing-flow.md` -> `Failpoint decision for unit tests` against the affected package.
- If any hit exists, rerun with `Failpoint-enabled run` and add `-count=1` to the `go test` command for reproducibility.
- If the diff disappears after failpoint enable, classify it as an environment/setup issue instead of a logic regression.

## Rule 2: isolate merge impact

If failpoint is not the cause:

1. Reproduce with minimal target (`-run <TestName> -count=1`).
2. Compare good/bad commits and bisect the merged range.
3. Identify first bad commit before updating expected outputs.

Useful commands:
```bash
git bisect start
git bisect bad <bad_commit>
git bisect good <good_commit>
```

## Rule 3: update testdata only after the cause is proven

Only sync expected plan/result when one of these is true:

- Upstream merge intentionally changed optimizer behavior.
- Existing test expectation is stale but query semantics are unchanged.

Do not record/update testdata before root cause is identified.

## Output format for investigation notes

- `Symptom`: what diff changed.
- `Scope`: single test vs full suite.
- `Failpoint check`: commands and result.
- `First bad commit`: hash and title (if bisected).
- `Conclusion`: setup issue / upstream behavior change / local regression.
- `Action`: rerun with failpoint, sync expected, or continue fixing code.

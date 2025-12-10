---
name: tidb-test-guidelines
description: Decide where to place TiDB tests and how to write them (basic structure, naming, testdata usage). Use when asked about test locations, writing conventions, shard_count limits, casetest categorization, or when reviewing test changes in code review.
---

# TiDB Test Placement and Writing

## Quick workflow

1) Identify the target package and existing coverage using `rg --files -g '*_test.go'` and `rg --files -g '*.json'`.
2) Check `BUILD.bazel` for `shard_count` in the target directory; keep test count <= 50 per directory.
3) For optimizer cases, place new tests under `pkg/planner/core/casetest/<type>`.
4) Reuse existing fixtures and testdata; add new files only when necessary.
5) Name tests descriptively; avoid issue-id-only names (e.g., `TestIssue123456`).
6) Merge same-functionality cases into a single test only if runtime remains reasonable.

## Basic writing rules

- Benchmarks (`func BenchmarkXxx`) are tests too; apply the same placement and naming rules (benchmarks do not count toward shard_count test limits).
- Prefer table-driven tests for related scenarios in the same behavior area; avoid `t.Run` in planner/core casetests to prevent concurrent subtest interference.
- Reuse existing helper setups and test fixtures; avoid re-creating schemas unless required.
- Prefer one `store` + one `testkit` per test; when a single test covers multiple scenarios, use distinct table names and restore any session/system variables to their original values.
- If a test must use multiple sessions or domains (for example, cross-session cache behavior), keep the extra stores/testkits but document why in the test.
- For planner/core casetests, prefer extending existing `testdata` suites in the same directory.
- Keep per-test runtime short; consolidate similar checks only if runtime stays reasonable.
- Use behavior-based test names; never use issue-id-only names.

## Placement rules

- **Optimizer tests**: Always under `pkg/planner/core/casetest/<type>` (see `references/planner-core-layout.md`).
- **Test count limit**: Keep <= 50 tests per directory; align with `shard_count` in the directory `BUILD.bazel`.
- **Naming**: Use behavior-based names (e.g., `TestJoinReorderKeepsOrder`), never issue-id-only names.
- **Consolidation**: Combine duplicate functionality into one test when possible, but keep total runtime reasonable.

## Reference files

- **Optimizer case map**: `references/optimizer-case-map.md`
- **Planner/core placement guide**: `references/planner-core-layout.md`

## Notes

- Apply the same rules (placement, shard_count, naming) to other packages beyond `pkg/planner`.
- Use existing testdata patterns (`*_in.json`, `*_out.json`, `*_xut.json`) in the same directory when extending suites.
- When moving benchmarks between packages, update any `TestBenchDaily` wrappers that list them and keep `Makefile` `bench-daily` entries aligned with the new package location.
- For planner/core plan cache benchmarks, keep them registered in `pkg/planner/core/casetest/plancache/plan_cache_test.go` `TestBenchDaily` after moving their definitions.
- When updating tests in any other directory, also update this skill: add or extend a case map under `references/` and add guidance in this `SKILL.md` so future changes stay consistent.

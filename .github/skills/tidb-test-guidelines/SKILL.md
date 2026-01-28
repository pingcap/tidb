---
name: tidb-test-guidelines
description: Decide where to place TiDB tests and how to write them (basic structure, naming, testdata usage). Use when asked about test locations, writing conventions, shard_count limits, casetest categorization, or when reviewing test changes in code review.
---

# TiDB Test Placement and Writing

## Quick workflow

1) Identify the target package and existing coverage using `rg --files -g '*_test.go'` and `rg --files -g '*.json'`.
2) Check `BUILD.bazel` for `shard_count` in the target directory; keep test count <= 50 per directory.
3) For optimizer cases, place new tests under `pkg/planner/core/casetest/<type>` (see `references/planner-guide.md`).
4) Reuse existing fixtures and testdata; add new files only when necessary.
5) Name tests descriptively; avoid issue-id-only names (e.g., `TestIssue123456`).
6) Merge same-functionality cases into a single test only if runtime remains reasonable.
7) When moving tests/benchmarks, update `BUILD.bazel` and `Makefile` (bench-daily) if needed.

## Basic writing rules

- Benchmarks (`func BenchmarkXxx`) are tests too; apply the same placement and naming rules (benchmarks do not count toward shard_count test limits).
- Prefer table-driven tests for related scenarios in the same behavior area.
- Reuse existing helper setups and test fixtures; avoid re-creating schemas unless required.
- Prefer one `store` + one `testkit` per test; when a single test covers multiple scenarios, use distinct table names and restore any session/system variables to their original values.
- If a test must use multiple sessions or domains (for example, cross-session cache behavior), keep the extra stores/testkits but document why in the test.
- Keep per-test runtime short; consolidate similar checks only if runtime stays reasonable.
- Use behavior-based test names; never use issue-id-only names.
- In test code, use the variable name `tk` for `*testkit.TestKit` (avoid `testKit`).
- When merging multiple tests into one, keep a single `store` and a single `tk` unless multi-session behavior is required; do not create a new store/tk inside the same test body without a documented reason.

## Placement rules

- **Test count limit**: Keep <= 50 tests per directory; align with `shard_count` in the directory `BUILD.bazel` (benchmarks are excluded).

## Reference files

- **Package case maps**: `references/<pkg>-case-map.md` for each top-level directory under `pkg/`
- **Planner core placement guide**: `references/planner-guide.md`

## Notes

- Apply the same rules (placement, shard_count, naming) to other packages beyond `pkg/planner`.
- Use existing testdata patterns (`*_in.json`, `*_out.json`, `*_xut.json`) in the same directory when extending suites. When tests use testdata, run with `-record --tags=intest` as needed.
- When moving benchmarks between packages, update any `TestBenchDaily` wrappers that list them and keep `Makefile` `bench-daily` entries aligned with the new package location.
- When updating tests in any `pkg/*` package, ask AI to update the corresponding case map under `references/`.
- When updating tests in any other directory, also update this skill: add or extend a case map under `references/` and add guidance in this `SKILL.md` so future changes stay consistent.
- When merging issue regression tests into existing behavior tests, keep the issue id in SQL comments (e.g. `/* issue:12345 */`) or nearby comments for traceability.
- Prefer unit tests over `tests/integrationtest` for end-to-end coverage unless you need to avoid union-storage executor differences or require full workflow validation.
- When tests read source files under Bazel, use `go/runfiles` and ensure the target file is exported via `exports_files()` in its owning `BUILD.bazel`.
- For Bazel runfiles, be ready to include the workspace prefix (from `TEST_WORKSPACE`) in the runfile path if needed.
- Validation (Bazel): run `make bazel_prepare` first; then check the package `BUILD.bazel` for `@com_github_pingcap_failpoint//:failpoint` dependency.
  - If present, run:
    - `make bazel-failpoint-enable`
    - `bazel test --norun_validations --define gotags=deadlock,intest --remote_cache=https://cache.hawkingrei.com/bazelcache --noremote_upload_local_results //path/to/package/...`
    - `make bazel-failpoint-disable`
  - If absent, run `bazel test` directly against the package path.

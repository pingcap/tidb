# Plan Cache Notes

## 2026-02-04: Bind Matching Cache for Plan Cache Key

### Key Points

- `NewPlanCacheKey` is hot; avoid per-execution normalization/digest work.
- Reuse cross-execution caches (`PlanCacheStmt.BindingInfo`) for binding normalization.
- Statement-scoped caches reset per statement and do not help plan cache key cost.
- Profiling regression signals: higher cum time in `NormalizeStmtForBinding`,
  `NormalizeDigestForBinding`, and `MatchSQLBinding` under `GetPlanFromPlanCache`.

## 2026-02-25: CTE Scope Tracking in Prepared Plan Cache Checker (Issue #66351)

### Symptoms Observed During Debugging

- Existing tests did not prove whether `SubqueryExpr` scope tracking was required.
- A crafted query shape failed only when a specific scope offset update was removed:
  scalar subquery + `WITH` + set operation, combined with an outer partitioned table.
- An early test draft produced a misleading error
  (`find table .t1 failed: [schema:1146]Table '.t1' doesn't exist`)
  because the SQL used `from t1` instead of `from test.t1` in this test context.

### Root Cause Summary

- `cacheableChecker` tracks CTE visibility using scoped offsets.
- Entering `*ast.SubqueryExpr` must push current `cteCanUsed` length into `cteOffset`.
- If that push is missing, CTE visibility can leak across subquery boundaries and trigger
  wrong table-resolution/cacheability behavior.

### Practical Lessons

- Prepared and non-prepared paths are not interchangeable test surfaces.
  Both paths may need coverage when the bug touches shared checker logic.
- Prefer adding regression cases to existing suites (`tidb-test-guidelines`) to reduce
  scaffolding and keep review focused.
- Keep test SQL fully qualified when parser/session default DB can vary in test setup.

### Regression Test Placement and Shape

- Appended a regression case to existing `TestCacheable` in
  `pkg/planner/core/casetest/plancache/plan_cacheable_checker_test.go`.
- Kept prepared-path validation via `TestPreparedPlanCacheWithCTE`.
- Representative SQL shape used for regression locking:
  `select (with t1 as (select 1 as a) select a from t1 union all select a from t1) as x from test.t1`

### Targeted Validation Commands

- `go test -count=1 -run TestCacheable -tags=intest,deadlock ./pkg/planner/core/casetest/plancache`
- `go test -count=1 -run TestPreparedPlanCacheWithCTE -tags=intest,deadlock ./pkg/planner/core/casetest/plancache`

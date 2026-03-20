# Plan Cache Notes

## 2026-02-04: Bind Matching Cache for Plan Cache Key

### Key Points

- `NewPlanCacheKey` is hot; avoid per-execution normalization/digest work.
- Reuse cross-execution caches (`PlanCacheStmt.BindingInfo`) for binding normalization.
- Statement-scoped caches reset per statement and do not help plan cache key cost.
- Profiling regression signals: higher cum time in `NormalizeStmtForBinding`,
  `NormalizeDigestForBinding`, and `MatchSQLBinding` under `GetPlanFromPlanCache`.

## 2026-02-25: Prepared Plan Cache Checker Notes (Issue #66351)

### Scope and Entry Points

- Source file: `pkg/planner/core/plan_cacheable_checker.go`.
- Prepared path entry: `IsASTCacheable` -> `cacheableChecker`.
- Non-prepared path entry: `NonPreparedPlanCacheableWithCtx` ->
  `nonPreparedPlanCacheableChecker`.
- Shared table-level checks are centralized in `checkTableCacheable`.

### Prepared Checker Traversal Rules

- `cacheableChecker` performs AST traversal and short-circuits once
  `checker.cacheable` becomes `false`.
- `skipForSubqueryDisabled()` is used by both `*ast.ExistsSubqueryExpr` and
  `*ast.SubqueryExpr` to keep the subquery gating logic in one place.
- `*ast.TableName` nodes go through InfoSchema validation, partition pruning
  mode checks, generated column checks, and temporary table checks.

### CTE Visibility Rules

- `cteCanUsed` stores CTE names visible in the current query block.
- `withScopeOffset` records CTE list boundaries for each `SelectStmt` that
  has a `WITH` clause, and `leaveWithScope()` restores the outer scope.
- `*ast.CommonTableExpression` handling:
  - Enter: recursive CTE names are pre-registered so self-reference can pass.
  - Leave: non-recursive CTE names are published after their query is visited.
- For `*ast.TableName` with empty schema and name in `cteCanUsed`, skip
  physical table lookup because the name refers to a CTE, not InfoSchema.

### Why Issue #66351 Happened

- The bug area is CTE name visibility while traversing nested query shapes.
- The practical failure pattern includes CTE + subquery/set-op combinations
  where checker state transitions are easy to break.
- The fix direction is to keep state transitions minimal and explicit, and lock
  behavior with regression tests in the existing plan cache checker suite.

### Test and Debugging Checklist

- Prefer adding cases into existing tests first:
  `pkg/planner/core/casetest/plancache/plan_cacheable_checker_test.go`.
- Verify both:
  - AST-level cacheability (`CacheableWithCtx`) for specific SQL shapes.
  - Prepared execution behavior (`@@last_plan_from_cache`) for runtime effect.
- Use fully qualified table names in regression SQL when test DB context can
  vary (`test.t1` instead of `t1`).
- If new tests are added, run `make bazel_prepare` before running tests.

### Targeted Validation Commands

- `go test -count=1 -run TestCacheable -tags=intest,deadlock ./pkg/planner/core/casetest/plancache`
- `go test -count=1 -run TestPreparedPlanCacheWithCTE -tags=intest,deadlock ./pkg/planner/core/casetest/plancache`

## 2026-03-11: Ranger Access Conditions Folded to Constant (Issue #63914)

### Scope and Entry Points

- Source file: `pkg/util/ranger/detacher.go`.
- Entry point: `detachCNFCondAndBuildRangeForIndex`.
- Regression test: `pkg/executor/prepared_test.go`, `TestPreparePlanCache4Function`.

### Why Issue #63914 Happened

- `ExtractEqAndInCondition` can fold a prefix access condition into
  `*expression.Constant` during prepared execution.
- The downstream range-detach path still assumes the access-condition prefix
  only contains `*expression.ScalarFunction`.
- When a folded constant remains inside `accessConds`, the later EQ-count loop
  panics on the type assertion, and the connection is torn down by the panic.

### Fix Direction

- Normalize `accessConds` immediately after extraction, before any range build
  or EQ-prefix counting.
- Treat folded `false` / `null` as an empty range result.
- Treat folded `true` as a broken prefix boundary and move suffix access
  conditions back to `newConditions` so they are not counted as prefix EQ/IN
  conditions.
- Keep the fix local to the proven root-cause path instead of adding ad-hoc
  handling for only the single-constant case.

### Regression Shape to Keep

- Prepared execution with repeated `<=>` predicates that fold to constants.
- Multi-column index shape where a suffix condition would otherwise stay in the
  prefix access chain after the folded constant.
- Sanity check that follow-up statements on the same connection still succeed.

### Targeted Validation Commands

- `make lint`
- `make failpoint-enable`
- `go test -count=1 -run TestPreparePlanCache4Function -tags=intest,deadlock ./pkg/executor`
- `make failpoint-disable`

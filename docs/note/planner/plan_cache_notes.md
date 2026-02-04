# Plan Cache Notes

## 2026-02-04: Bind Matching Cache for Plan Cache Key

### Background

Two changes (PRs #65484 and #65766) introduced a performance regression in `BenchmarkNonPartitionPointGetPlanCacheOn`.
Profiling showed the hot path moved into `core.GetPlanFromPlanCache`, dominated by repeated
`NormalizeStmtForBinding` / `NormalizeDigestForBinding` and `bindinfo.MatchSQLBinding` work.

### Root Cause

`NewPlanCacheKey` switched to `bindinfo.MatchSQLBinding` which does not reuse
`PlanCacheStmt.BindingInfo` (normalization cache). The stmt-level cache (`StmtCtx.MatchSQLBindingCache`)
is reset per statement and does not help for plan cache key creation, so the normalization cost
was paid on every execution.

### Fix

Restore normalization caching by reusing `BindingMatchInfo` in plan cache key creation:

- Export `bindinfo.MatchSQLBindingWithCache(sctx, stmtNode, info)` and share the common
  cache path with `MatchSQLBinding`.
- In `NewPlanCacheKey`, call `MatchSQLBindingWithCache` with `&stmt.BindingInfo`
  and use the matched `BindSQL` for the cache key.

This makes plan cache key creation reuse normalized digest and table name collection.

### Verification

Benchmark:

```bash
go test -run ^$ -bench BenchmarkNonPartitionPointGetPlanCacheOn -count=50 -timeout 40m ./pkg/planner/core/tests/partition
```

Profile:

```bash
go test -run ^$ -bench BenchmarkNonPartitionPointGetPlanCacheOn -count=50 -timeout 40m \
  -cpuprofile /tmp/bench_non_partition_point_get_plan_cache_on_50_fixed.cpu \
  ./pkg/planner/core/tests/partition
go tool pprof -top -cum -focus GetPlanFromPlanCache partition.test \
  /tmp/bench_non_partition_point_get_plan_cache_on_50_fixed.cpu
```

Expected outcome: time under `GetPlanFromPlanCache` drops back near pre-regression levels.

### Regression Evidence

Compare the slow profile (post-PRs) against the fixed profile:

```bash
go tool pprof -top -cum -focus GetPlanFromPlanCache -base bench_non_partition_point_get_plan_cache_on_50.cpu \
  partition.test /tmp/bench_non_partition_point_get_plan_cache_on_50_fixed.cpu
```

Expected outcome: negative deltas in `MatchSQLBinding`, `NormalizeStmtForBinding`,
and `NormalizeDigestForBinding`, indicating the regression is removed.

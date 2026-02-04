# Plan Cache Notes

## 2026-02-04: Bind Matching Cache for Plan Cache Key

### Key Learnings

- `NewPlanCacheKey` is a hot path. Any extra normalization or digest work here is amplified.
- Use cross-execution caches for binding normalization. `PlanCacheStmt.BindingInfo` can reuse
  `NormalizeStmtForBinding` and `CollectTableNames` results across executions.
- Statement-scoped caches (`StmtCtx.MatchSQLBindingCache`) reset every statement and do not
  reduce plan cache key costs in single-call paths.
- Avoid calling `MatchSQLBinding` without reuse when building cache keys; pass an explicit
  normalization cache instead.
- In profiling, a regression will show up as higher cum time in
  `NormalizeStmtForBinding`, `NormalizeDigestForBinding`, and `MatchSQLBinding`
  under `GetPlanFromPlanCache`.

### Practices

- Provide a cache-aware entry point (for example `MatchSQLBindingWithCache(sctx, stmtNode, info)`),
  and keep `MatchSQLBinding` on the shared code path.
- Keep cache ownership with `PlanCacheStmt` for plan cache key construction.

### Validation (Example)

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

Regression evidence (slow vs fixed):

```bash
go tool pprof -top -cum -focus GetPlanFromPlanCache -base bench_non_partition_point_get_plan_cache_on_50.cpu \
  partition.test /tmp/bench_non_partition_point_get_plan_cache_on_50_fixed.cpu
```

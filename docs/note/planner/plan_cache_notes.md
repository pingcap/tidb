# Plan Cache Notes

## 2026-02-04: Bind Matching Cache for Plan Cache Key

### Key Points

- `NewPlanCacheKey` is hot; avoid per-execution normalization/digest work.
- Reuse cross-execution caches (`PlanCacheStmt.BindingInfo`) for binding normalization.
- Statement-scoped caches reset per statement and do not help plan cache key cost.
- Profiling regression signals: higher cum time in `NormalizeStmtForBinding`,
  `NormalizeDigestForBinding`, and `MatchSQLBinding` under `GetPlanFromPlanCache`.

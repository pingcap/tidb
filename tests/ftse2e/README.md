# Local MATCH plan validation unit tests

This package contains 19 deterministic plan-shape tests. It requires no running cluster, DSN, global variable changes, or opt-in environment variables.

```bash
go test ./tests/ftse2e -count=1 -v
```

The cases cover MPP TableFullScan/TableRangeScan placement, unrelated root filters, forbidden root MATCH residuals, missing MATCH, incorrect scan placement, TiDB root fallback, TiCI and cop/batchCop rejection, and the MPP envelope required for decoded statement-summary plans.

These fixtures validate the checker, not an actual deployment. Real-cluster acceptance must independently wait for replica readiness, compare complete results, and check actual MATCH execution placement. Real-cluster E2E, MySQL compatibility oracles, performance/profile tools, and pressure workloads are maintained outside the product PR for integration into the test system. This package contains no microbenchmarks or deployment scripts.

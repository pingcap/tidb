# Prepared Plan Cache Test Reorg Notes

## 2026-03-11

### Goal

Reduce the spread of prepared plan cache tests across `pkg/executor/prepared_test.go`,
`pkg/executor/test/seqtest/prepared_test.go`, and `pkg/executor/test/plancache/plan_cache_test.go`,
while keeping regression coverage and making future issue filing easier.

### Current layout

- `pkg/executor/prepared_test.go`
  - Mixed suite.
  - Contains both core prepared-statement semantics and prepared plan cache regressions.
  - Also owns the `prepareMergeSuiteData` loader used by some testdata-backed plan cache cases.
- `pkg/executor/test/seqtest/prepared_test.go`
  - Sequential prepared-statement semantics and DML behavior.
  - Large overlap with the older generic prepared cases in `pkg/executor/prepared_test.go`.
- `pkg/executor/test/plancache/plan_cache_test.go`
  - Executor-side prepared plan cache fast-path coverage.
  - Currently focuses on point get / point update and commit-mode behavior.

### Overlap observed

- `pkg/executor/prepared_test.go` and `pkg/executor/test/seqtest/prepared_test.go` both cover base prepared-statement behavior.
- `pkg/executor/prepared_test.go` contains several tests that are semantically closer to `pkg/executor/test/plancache` than to the generic executor package:
  - null-parameter cache plan selection
  - point-get/range/or plan selection regressions
  - composite-index cached access regressions
  - optimizer/session-variable interactions with prepared plan cache
  - binding and foreign-key invalidation regressions
  - clustered-index prepared plan cache regressions
  - broad operator coverage for prepared plan cache

### Constraints discovered during the reorg

- Some plan cache tests in `pkg/executor/prepared_test.go` are not plain executor regressions:
  - `TestPlanCacheWithDifferentVariableTypes`
  - `TestParameterPushDown`
  These rely on `prepareMergeSuiteData` from `pkg/executor/main_test.go`.
- `TestPrepareStmtAfterIsolationReadChange` is heavier than the others:
  - requires `CreateMockStoreAndDomain`
  - mutates virtual TiFlash replica metadata
  - validates prepared-statement behavior after isolation-read changes rather than session plan-cache reuse
- `pkg/executor/test/plancache/main_test.go` does not currently load any testdata suite.

### Rejected approach

The first attempted direction was a near 1:1 move of many plan-cache-related tests from
`pkg/executor/prepared_test.go` into a new file under `pkg/executor/test/plancache/`.

Why it was rejected:

- It preserved too many tables and too many issue-shaped test cases.
- It improved directory ownership but did not improve readability or maintenance cost.
- It did not satisfy the stronger requirement to merge tests with the fewest tables and the fewest cases that still cover the same behavior families.

### Landed reorg shape

The implemented reorg keeps `pkg/executor/test/plancache/plan_cache_test.go` as the landing spot and groups the migrated cases into behavior-oriented suites, each with one shared `store` and one shared `tk`.

Current split:

1. `TestPreparedPlanCachePlanSelectionRegressions`
   - Keep a minimal set of schemas.
   - Cover:
     - null parameter -> `TableDual`
     - point get vs range fallback
     - `OR` shape that must not incorrectly reuse a point-get plan
     - execution-info toggle that should not break a valid cache hit
     - one composite-index lookup regression
     - one join-plan selection regression

2. `TestPreparedPlanCacheSessionInteractions`
   - Reuse one small table family where possible.
   - Cover:
     - optimizer-rule blacklist / expression-pushdown blacklist
     - nondeterministic function reuse
     - control-function non-cacheability
     - cache-key-sensitive session variables
     - binding invalidation / binding hit
     - foreign-key invalidation for both text and protocol prepared execution

3. `TestPreparedPlanCacheClusteredIndex`
   - Keep a single clustered-index table.
   - Cover:
     - clustered-index range
     - clustered-index point get
     - clustered-index batch point get

4. `TestPreparedPlanCacheOperators`
   - Replace the broad matrix with one representative case per behavior family.
   - Keep the operator families, but shrink the case count:
     - point get / reader
     - index lookup
     - join hint path
     - apply non-cacheable path
     - window cacheable path
     - window non-cacheable path
     - limit non-cacheable path
     - order cacheable path
     - top-N non-cacheable path

### Cases that should stay in `pkg/executor/prepared_test.go` for now

- `TestPlanCacheWithDifferentVariableTypes`
  - Depends on existing root-package testdata wiring.
- `TestParameterPushDown`
  - Same reason as above.
- `TestPrepareStmtAfterIsolationReadChange`
  - More about prepared-statement engine selection and normalized metadata than executor-side prepared plan cache reuse.
- Core prepared-statement semantics that are not plan-cache specific:
  - max prepared statement count
  - wrong-type execution
  - protocol charset regression

### Files changed in the reorg

- `pkg/executor/prepared_test.go`
- `pkg/executor/test/plancache/plan_cache_test.go`
- `.agents/skills/tidb-test-guidelines/references/executor-case-map.md`

`BUILD.bazel` was intentionally left untouched because the reorg stayed within existing files. That avoided a `make bazel_prepare` round and kept the diff smaller.

### Why this note exists

This note is intended to be copied into or referenced by a follow-up issue/PR description, so the next pass does not need to rediscover:

- where the duplication is
- which tests are cheap to move
- which tests should stay temporarily
- why a direct mechanical move is the wrong end state
- why the landed version stopped at four grouped suites instead of one giant merged test

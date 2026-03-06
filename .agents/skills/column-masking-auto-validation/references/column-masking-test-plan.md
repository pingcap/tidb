# Column Masking Test Plan

## 1. Goal and Scope

This plan validates the TiDB column masking feature for:

- correctness
- security and permission boundaries
- DDL lifecycle consistency
- observability and metadata consistency
- compatibility across key SQL/runtime features
- upgrade/downgrade impact

Primary feature scope:

- `CREATE/ALTER/DROP MASKING POLICY`
- `AT RESULT` semantics (compute on original values, return masked values)
- `RESTRICT ON` runtime controls
- masking functions (`MASK_FULL`, `MASK_PARTIAL`, `MASK_NULL`, `MASK_DATE`)
- visibility surfaces (`SHOW MASKING POLICIES`, `SHOW CREATE TABLE`, `mysql.tidb_masking_policy`)

Out of scope for this plan:

- standalone log-redaction feature validation
- full toolchain performance benchmark framework (only targeted gates here)

## 2. Test Strategy

Use automated tests as default:

- unit tests for function logic and planner/DDL internals
- integration tests for SQL-visible behavior and end-to-end semantics

Execution policy:

- prioritize deterministic targeted tests
- avoid one-off ad-hoc scripts for behavioral validation
- generate a scenario-level report automatically from the skill

## 3. Coverage Model

### 3.1 Priority Levels

- `P0`: must-have for GA decision
- `P1`: strong recommendation before GA
- `P2`: ecosystem/toolchain and longer-path validation

### 3.2 P0 Scenario Groups

- `P0-DDL`: lifecycle, constraints, unsupported objects/columns, binding stability
- `P0-AUTH`: dynamic privileges and identity-function semantics
- `P0-CORE`: `AT RESULT` correctness in query pipeline
- `P0-RES`: `RESTRICT ON` deny/allow behavior
- `P0-FUNC`: masking builtin behavior and boundaries
- `P0-OBS`: metadata and `SHOW` consistency

### 3.3 Test Surfaces

- Integration:
  - `tests/integrationtest/t/privilege/column_masking_policy.test`
- Unit:
  - `pkg/ddl/masking_policy_test.go`
  - `pkg/planner/core/masking_policy_projection_test.go`
  - `pkg/planner/core/masking_policy_restrict_test.go`
  - `pkg/planner/core/masking_policy_expr_cache_test.go`
  - `pkg/executor/show_test.go`
- Integration (masking builtins):
  - `tests/integrationtest/t/expression/builtin.test`

### 3.4 P1 Scenario Groups

- `P1-CACHE`: prepared statement / schema cache invalidation after policy and column metadata changes
- `P1-COMPAT`: partition tables and transaction mode intersections
- `P1-RES`: `RESTRICT ON` behavior with prepared DML statements

## 4. Compatibility and Intersections

This plan explicitly tracks intersections with:

- SQL operators and clauses (`WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`, projection expressions)
- prepared statements and plan/schema cache invalidation
- transaction modes (pessimistic/optimistic)
- partitioned tables and index access paths
- observability features (`SHOW`, statement summary, slow log)
- management/tooling surfaces listed in TiDB basic features

Reference:

- https://docs.pingcap.com/zh/tidb/stable/basic-features/
- https://docs.pingcap.com/zh/tidb/stable/basic-features/#管理可视化和工具

## 5. Performance and Stability Gates

### 5.1 Performance Gates

- single-column masking query: latency/QPS regression gate
- multi-column masked projection: latency/CPU regression gate
- `RESTRICT ON` rejection path: stable rejection latency and error behavior
- masking builtin micro-benchmark trend monitoring

### 5.2 Stability Gates

- long-run mixed workload (reads + restricted writes + policy changes)
- high-frequency policy toggles and updates (no cache stale behavior)
- large policy-metadata volume operations

## 6. Version and Lifecycle Validation

Required paths:

- upgrade from pre-feature versions to feature versions
- patch upgrade within feature-enabled versions
- rolling upgrade with mixed binaries
- downgrade strategy validation (block or pre-cleanup, per product definition)
- BR/PITR restore with policy metadata and identity semantics checks

## 7. Known Constraints / N-A Areas

- `CREATE TABLE ... SELECT` (`CTAS`) may be unsupported in the current branch; mark related scenario `N/A` until implemented.
- if implementation has known open behavior (for example some rename edge paths), mark as `PARTIAL` with evidence in the generated report.

## 8. Acceptance Criteria (GA-Facing)

Minimum for GA decision:

1. all `P0` scenarios are automated and mapped to concrete test surfaces
2. generated report clearly shows `PASS/FAIL/PARTIAL/NOT_COVERED/N/A` by scenario
3. high-risk `P1` intersections (cache, transaction, partition, prepare) are covered and pass
4. no unresolved correctness/security blockers in report open items

## 9. Execution and Reporting Workflow

Run using this skill:

```bash
./.agents/skills/column-masking-auto-validation/scripts/run_validation.sh
```

Optional strict mode:

```bash
./.agents/skills/column-masking-auto-validation/scripts/run_validation.sh --with-bazel-prepare --with-lint
```

Output model:

- human-facing plan: this document
- machine-executed evidence: artifacts logs and step records
- human-facing result: autogenerated report in artifacts directory (`column-masking-report.md`)

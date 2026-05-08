# Planner Core Placement Guide

## Purpose

Define placement rules for optimizer tests under `pkg/planner/core`, with casetests grouped by type.

## Preferred layout

- **Planner/core optimizer casetests**: `pkg/planner/core/casetest/<type>`
- **Testdata for optimizer cases**: `pkg/planner/core/casetest/<type>/testdata`
- **Avoid** adding new optimizer cases directly under `pkg/planner/core` or `pkg/planner/core/tests` unless there is a strong, existing test suite to extend.

## Other planner packages

Planner/core and non-core packages are peers. Place tests in their owning package directory when the code is not part of `planner/core`:

- **Planner core (core capabilities, logical and physical optimization)**: `pkg/planner/core`.
- **Cardinality**: `pkg/planner/cardinality`
- **Functional dependencies**: `pkg/planner/funcdep`
- **Memo**: `pkg/planner/memo`
- **Plan context**: `pkg/planner/planctx`
- **Planner utilities**: `pkg/planner/util`
- **Index advisor**: `pkg/planner/indexadvisor`
- **Implementation (interface definitions)**: `pkg/planner/implementation`
- **Cascades (old/base/pattern/memo/task/rule)**: `pkg/planner/cascades/...`

## Common casetest types

- `binaryplan`
- `cascades`
- `cbotest`
- `ch` (CH benchmarks and datasets)
- `correlated`
- `dag`
- `enforcempp`
- `flatplan`
- `hint`
- `index`
- `indexmerge`
- `instanceplancache`
- `join`
- `logicalplan`
- `mpp`
- `parallelapply`
- `partition`
- `physicalplantest`
- `plancache`
- `planstats`
- `pushdown`
- `rule`
- `scalarsubquery`
- `tpcds`
- `tpch`
- `vectorsearch`
- `windows`

## When adding a new type

- Create `pkg/planner/core/casetest/<type>/main_test.go` and one or more `*_test.go` files.
- Add or extend `testdata` in the same directory.
- Keep test count per directory <= 50 and align `BUILD.bazel` `shard_count`.

## Naming and consolidation

- Use behavior-based names; never name tests after issue IDs only.
- Merge overlapping functionality into a single test only if it does not make runtime too long.

## Planner-specific rules

- For optimizer cases, always place new tests under `pkg/planner/core/casetest/<type>` and keep testdata in `pkg/planner/core/casetest/<type>/testdata`.
- Avoid adding new optimizer cases directly under `pkg/planner/core` or `pkg/planner/core/tests` unless extending an existing suite.

## Planner explain-format conventions

- For ordinary planner/core casetests that assert plan shape, prefer `EXPLAIN format = 'plan_tree'`.
- Keep explain-format-specific suites unchanged; do not bulk-rewrite tests whose purpose is to validate another explain format.
- Do not switch stats/analyze/load-stats-related tests to `plan_tree`. This includes `pkg/planner/core/casetest/stats_test.go`, `pkg/planner/core/casetest/planstats`, analyze-focused CBO suites under `pkg/planner/core/casetest/cbotest`, and any suite whose setup depends on `load stats`. These tests are format-sensitive, and `EXPLAIN ANALYZE format = 'plan_tree'` is not supported.
- For cost-oriented assertions, prefer `EXPLAIN format = 'cost_trace'` rather than `plan_tree`.
- For plan cache tests, preserve the existing `explain for connection` / `EXPLAIN format='plan_cache'` style; do not rewrite those assertions to `plan_tree`.
- For hint tests, prefer `EXPLAIN format = 'plan_tree'` when the assertion is about the final chosen plan. Keep the nearby suite's existing format only for warning text, hint applicability, or other format-sensitive output.

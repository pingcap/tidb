# Planner/Core Placement Guide

## Purpose

Keep optimizer tests organized and discoverable by forcing them into `pkg/planner/core/casetest/<type>`.

## Preferred layout (optimized paths)

- **All new optimizer test cases**: `pkg/planner/core/casetest/<type>`
- **Testdata for optimizer cases**: `pkg/planner/core/casetest/<type>/testdata`
- **Avoid** adding new optimizer cases directly under `pkg/planner/core` or `pkg/planner/core/tests` unless there is a strong, existing test suite to extend.

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

# Planner Core Placement Guide

## Purpose

Define placement rules for optimizer tests under `pkg/planner/core`, with casetests grouped by type.

## Preferred layout

- **Planner/core optimizer casetests**: `pkg/planner/core/casetest/<type>`
- **Testdata for optimizer cases**: `pkg/planner/core/casetest/<type>/testdata`
- **Avoid** adding new optimizer cases directly under `pkg/planner/core` or `pkg/planner/core/tests` unless there is a strong, existing test suite to extend.

## Other planner packages

Planner/core and non-core packages are peers. Place tests in their owning package directory when the code is not part of `planner/core`:

- **Cardinality**: `pkg/planner/cardinality`
- **Functional dependencies**: `pkg/planner/funcdep`
- **Memo**: `pkg/planner/memo`
- **Plan context**: `pkg/planner/planctx`
- **Planner utilities**: `pkg/planner/util`
- **Index advisor**: `pkg/planner/indexadvisor`
- **Implementation**: `pkg/planner/implementation`
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

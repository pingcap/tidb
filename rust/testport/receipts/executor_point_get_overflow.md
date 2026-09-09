# `pkg/planner/core/point_get_plan.go` out-of-range point constants

Status: Ready for this focused point-get fast-path batch. The claim unit is
the `isTableDual` arm of Go's `tryPointGetPlan`, not the complete
`pkg/planner/core` package.

## Go authority

Go source: `origin/master` `f5cf8f6337`. `getNameValuePairs`
(`pkg/planner/core/point_get_plan.go:1012-1016`) converts each equality
constant into its column's field type. When `ConvertTo` reports
`types.ErrOverflow`, it keeps the ORIGINAL datum and returns
`isTableDual = true`; `tryPointGetPlan` (`:579-583`, `:630-646`) then plans a
`TableDual`, and `TryFastPlan` (`:127-133`) turns that into
`PhysicalTableDual`. The comparison can never hold, so the answer is empty
and no storage read runs. Both artifacts were read in full; there is no
generated, fixture, or platform variant for this arm.

## Gap

Rust converted the constant in `point_get_value`
(`crates/tidb-executor/src/driver/point_get_key.rs`) and returned `None` on
any failure, so the fast plan declined and the ordinary planner answered an
out-of-range equality with `Projection -> Selection -> TableReader ->
TableFullScan`: the same empty rows, but a full table scan Go never performs.

## Fix

`point_get_value_overflowed` distinguishes Go's `ErrOverflow` arm (the
`ScalarConversionEvent::Overflow` event `convert_to` records while clamping)
from a non-representable-but-finite value such as `1.5`. Both point paths now
answer the empty set:

- the prepared path (`PreparedPointGetPlan::bind`) returns an empty execution
  next to its existing `handle = NULL` and over-length-string arms;
- the plain path (`try_fast_point_physical_plan_with_allocator_mode`) plans
  the wired `physical::PhysicalTableDual` when
  `point_get_predicate_overflows` finds an overflowing equality. That helper
  repeats `try_point_get`'s `HAVING`/`ORDER BY`/`GROUP BY`/removing-`LIMIT`
  guards, so it fires only where Go's `tryPointGetPlan` reaches
  `getNameValuePairs` at all.

## Fail-before / pass-after

- `driver::tests::point_get::prepared_point_plan_answers_an_out_of_range_handle_without_reading`
  failed at `bind(...)` returning `None` before the fix and passes after.
- `driver::tests::point_get::out_of_range_point_literal_plans_a_table_dual`
  failed with the `Projection/Selection/TableFullScan` plan before the fix
  and now sees `TableDual`.
- `driver::tests::point_get::out_of_range_point_literal_with_order_by_stays_with_the_planner`
  pins the reachability guard: the same predicate with `ORDER BY` keeps the
  ordinary plan, as Go's earlier refusal does.
- `point_get_key::tests::an_out_of_range_constant_overflows_its_column_domain`
  pins the event-level predicate, including that a too-long string is a
  TRUNCATION (Go's `ErrTruncatedWrongVal`) rather than an overflow.

## Validation

Profile: **Ready** for this focused batch.

- `cargo test --offline --locked -j12 -p tidb-executor --lib point_get_key` —
  13 passed.
- `cargo test --offline --locked -j12 -p tidb-executor --lib point_get` —
  55 passed, 5 failed. The 5 are the branch's pre-existing failures; the
  failure set is byte-identical before and after this batch.
- `cargo test --offline --locked -j12 -p tidb-executor --lib` —
  `1111 passed; 114-115 failed` across runs, against the branch's earlier
  `1107 passed; 115 failed`. The varying failure is the branch's known-flaky
  `access_cost::index_async_load_queue_tests::a_fully_loaded_column_is_not_queued`;
  no failure is in this batch's files and no new failing test appeared.
- `cargo fmt --all -- --check` — the changed files are rustfmt-clean (the
  branch's pre-existing drift is in `ddl.rs`, `ddl/alter_table.rs`, and
  `cluster_session_node/mod.rs`, untouched here); `git diff --check` passed.

No Go, Bazel, Cargo manifest, generated, or fixture file changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: an overflowing equality can equal no stored value, so the
  empty answer is sound; the check only fires on the conversion's overflow
  event, never on a value that rounds or truncates into range.
- Compatibility: the prepared path's existing `NULL`/over-length arms are
  unchanged and still run first for their cases.
- Performance: replaces a full scan with no read for the overflowing case.

# b102 baseline (clean tree, before any edits)

Command: `cargo nextest run --locked -p tidb-executor -E 'not test(/bench/)' --no-fail-fast` (cwd `rust/`)

Summary [  57.583s] 1273 tests run: 1264 passed, 9 failed, 7 skipped

Baseline failing set (9, all pre-existing driver::tests failures; the landed
b103 commit message 8412aa3b20 describes the same 9 on its branch):

- tidb-executor::driver::tests::aggregates::global_count_over_index_ranges_uses_gos_stream_agg_and_index_reader
- tidb-executor::driver::tests::aggregates::grouped_partial_count_carries_the_group_key
- tidb-executor::driver::tests::aggregates::tpcc_condition_two_orders_group_uses_the_covering_index_range
- tidb-executor::driver::tests::aggregates::tpcc_grouped_common_handle_uses_partial_and_final_stream_agg
- tidb-executor::driver::tests::aggregates::tpcc_condition_four_streams_across_a_grouped_derived_table
- tidb-executor::driver::tests::aggregates::tpcc_condition_six_simplifies_and_pushes_through_derived_tables
- tidb-executor::driver::tests::joins::tpcc_check_seven_propagates_the_warehouse_range_to_both_leaves
- tidb-executor::driver::tests::point_get::residual_selection_uses_logical_rows_over_access_rows
- tidb-executor::driver::tests::subqueries::tpcc_conditions_ten_and_twelve_decorrelate_scalar_sums

## Batch scope (derived from MANIFEST b102 = pkg/ddl.part3 = items 121-180 of
the deterministic Test*/Benchmark* enumeration, sorted by path then line)

Part2 (b101) ended at item 120 = pkg/ddl/db_change_test.go:995
(TestParallelAlterModifyColumnWithData); part4 (b103) began at item 181 =
pkg/ddl/db_integration_test.go TestChangeColumnPosition. So part3 is:

pkg/ddl/db_change_test.go (32): lines 1068, 1124, 1141, 1172, 1194, 1215,
1228, 1257, 1285, 1299, 1312, 1329, 1341, 1353, 1365, 1378, 1391, 1404, 1540,
1548, 1556, 1573, 1599, 1649, 1667, 1681, 1694, 1715, 1784, 1887, 1939, 2048

pkg/ddl/db_integration_test.go (28): lines 60, 87, 98, 115, 133, 143, 166,
177, 201, 238, 257, 284, 373, 398, 422, 442, 468, 626, 661, 678, 695, 712,
729, 746, 766, 824, 851, 948

## Session corrections (append-only)

- WITHDRAWN: an early working assumption that Go's
  `errno.ErrTooBigPrecision` is 1425 with message `Too big precision ...
  for column ...`. origin/master pkg/errno/errcode.go:429 pins 1426 and
  pkg/errno/errname.go:435 pins the `Too-big precision %d specified for
  '%-.192s'. Maximum is %d.` wording; the Rust carrier is an exact match.
  TestTableDDLWithTimeType is therefore a running port, not a gap.

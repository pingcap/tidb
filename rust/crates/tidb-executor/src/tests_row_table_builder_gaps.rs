// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap inventory for Go `pkg/executor/join/row_table_builder_test.go`.
//! These tests call the private `rowTableBuilder.processOneChunk` through a
//! hand-built `HashJoinCtxV2` and inspect tagged row-table pointers. The Rust
//! safe data-layer port is in the lower `tidb-exec` crate, not reachable from
//! this owning crate, and does not expose the Go white-box fixture.

/// Go `pkg/executor/join/row_table_builder_test.go:161::TestKey`; key bytes and valid-row sentinels are checked by `checkKeys` at `row_table_builder_test.go:68`.
#[test]
#[ignore = "go-parity-gap: processOneChunk, HashJoinCtxV2, and tagged row tables are not exposed here"]
fn row_table_builder_serializes_all_key_layouts() {}

/// Go `pkg/executor/join/row_table_builder_test.go:415::TestColumnsBasic`; the conversion matrix is implemented by `checkColumns` at `row_table_builder_test.go:292`.
#[test]
#[ignore = "go-parity-gap: the processOneChunk column fixture matrix is unported"]
fn row_table_builder_converts_only_needed_columns() {}

/// Go `pkg/executor/join/row_table_builder_test.go:460::TestColumnsAllDataTypes`; the seventeen-type matrix is declared at `row_table_builder_test.go:460` and validated by `checkColumnResult` at line 260.
#[test]
#[ignore = "go-parity-gap: all-MySQL-type row-table fixtures and raw-column checks are unported"]
fn row_table_builder_round_trips_all_data_types() {}

/// Go `pkg/executor/join/row_table_builder_test.go:518::TestBalanceOfFilteredRows`; partition counts are checked after `processOneChunk` at `row_table_builder_test.go:554`.
#[test]
#[ignore = "go-parity-gap: HashJoinCtxV2 partition setup and filtered-row balancing are unported"]
fn row_table_builder_balances_filtered_rows() {}

/// Go `pkg/executor/join/row_table_builder_test.go:561::TestUnalignmentLoad`; the source compares unsafe uint64/uint32/uint8 loads at `row_table_builder_test.go:570`.
#[test]
#[ignore = "go-parity-gap: the source pins unsafe unaligned loads, while this workspace has no safe Rust counterpart"]
fn row_table_builder_unaligned_loads_match_aligned_loads() {}

/// Go `pkg/executor/join/row_table_builder_test.go:601::TestSetupPartitionInfo`; expected concurrency geometry is declared at `row_table_builder_test.go:602`.
#[test]
#[ignore = "go-parity-gap: the ported partition geometry is in tidb-exec and is outside this crate's test boundary"]
fn row_table_builder_partition_info_matches_concurrency_geometry() {}

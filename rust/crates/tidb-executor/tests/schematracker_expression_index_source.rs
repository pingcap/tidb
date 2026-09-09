// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Ports of the five `pkg/ddl/schematracker/dm_tracker_test.go` (master)
//! functions past `TestBitDefaultValues` -- the slice of the pkg/ddl batch
//! this module owns. Go drives `schematracker.SchemaTracker`, the offline
//! DDL-to-TableInfo translator used by data-migration tooling; the package is
//! not transcreated in this tier, so every test is recorded as an explicit
//! gap with its contract re-derived from the Go source. Nothing is
//! approximated. (The tracker's first ten tests are recorded in the same
//! shape by `schematracker_dm_source.rs` from batch b109.)

/// Go `TestAddExpressionIndex`
/// (`pkg/ddl/schematracker/dm_tracker_test.go:469`): expression indexes
/// through the tracker -- `add index idx((a+b))` and a multi-column
/// `idx_multi((a+b),(a+1), b)` round-trip through SHOW CREATE TABLE text;
/// dropping them restores the bare table; a UNIQUE expression index on
/// `(concat(a, b))` and `alter index ... invisible` keep their text; a CREATE
/// TABLE with five inline expression indexes auto-names the unnamed ones
/// `expression_index`, `expression_index_2`, ... in declaration order
/// (`expression_index_4` for the UNIQUE); and a RANGE-partitioned table takes
/// an expression index with the partition clause preserved in SHOW CREATE.
// go-parity-gap: no SchemaTracker carrier (the package is not transcreated)
// and no SHOW CREATE TABLE text renderer in this tier.
#[test]
#[ignore]
fn schema_tracker_expression_index_round_trips_show_create_text() {
}

/// Go `TestAtomicMultiSchemaChange`
/// (`pkg/ddl/schematracker/dm_tracker_test.go:576`): `add b int, add c int`
/// lands both columns; `add d int, add a int` fails with
/// `infoschema.ErrColumnExists` (the duplicate `a`) and the table still has
/// exactly 3 columns -- a multi-action ALTER is ATOMIC, the successful
/// prefix is not kept.
// go-parity-gap: no SchemaTracker carrier.
#[test]
#[ignore]
fn schema_tracker_multi_schema_change_is_atomic_on_duplicate_column() {
}

/// Go `TestImmutableTableInfo`
/// (`pkg/ddl/schematracker/dm_tracker_test.go:603`): a `*model.TableInfo`
/// fetched BEFORE an ALTER stays byte-identical across it -- `alter table ...
/// comment = '123'` and `convert to character set utf8mb4 collate
/// utf8mb4_general_ci` update only a freshly-fetched copy (comment, table
/// and column charset/collation latin1 -> utf8mb4/utf8mb4_general_ci); the
/// old pointer still reports the empty comment and latin1 everywhere.
// go-parity-gap: no SchemaTracker carrier (the tracker's copy-on-fetch
// contract has no Rust surface here).
#[test]
#[ignore]
fn schema_tracker_table_info_fetched_before_an_alter_stays_immutable() {
}

/// Go `TestModifyFromNullToNotNull`
/// (`pkg/ddl/schematracker/dm_tracker_test.go:660`): `modify column a int not
/// null` succeeds WHEN the caller supplies a RestrictedSQLExecutor (the
/// NULL-to-NOT-NULL data check needs one) and keeps the column count at 2.
// go-parity-gap: no SchemaTracker carrier and no RestrictedSQLExecutor seam.
#[test]
#[ignore]
fn schema_tracker_modify_null_to_not_null_needs_a_restricted_executor() {
}

/// Go `TestDropListPartition`
/// (`pkg/ddl/schematracker/dm_tracker_test.go:681`): a LIST-partitioned table
/// (`PARTITION BY LIST (store_id)` with four `VALUES IN` partitions and a
/// composite primary key) takes `ALTER TABLE ... DROP PARTITION pEast`.
// go-parity-gap: no SchemaTracker carrier.
#[test]
#[ignore]
fn schema_tracker_drops_a_list_partition() {
}

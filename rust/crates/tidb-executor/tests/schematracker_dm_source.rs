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

//! Ports of Go `pkg/ddl/schematracker/dm_tracker_test.go` (pkg/ddl batch):
//! ten `Test*` functions driving `schematracker.SchemaTracker` -- Go's
//! offline DDL-to-TableInfo translator used by data-migration tooling. The
// tracker package is not transcreated in this tier, so every test is
// recorded as an explicit gap with its contract re-derived from the Go
// source. Nothing is approximated.

/// Go `TestNoNumLimit` (`pkg/ddl/schematracker/dm_tracker_test.go:51`): the
/// tracker creates a 3000-column table, a table with 100 columns + 100
/// indexes, and then ALTERs the wide table -- the schema-limit checks that
/// gate a real server (maximum columns/indexes per table) do not apply to
/// tracker-built metadata.
// go-parity-gap: no SchemaTracker carrier (the package is not
// transcreated).
#[test]
#[ignore]
fn schema_tracker_ignores_server_table_size_limits() {
}

/// Go `TestCreateTableLongIndex`
/// (`pkg/ddl/schematracker/dm_tracker_test.go:84`): index prefixes longer
/// than the server-side key-length limit (blob(555555), varchar chains)
/// build fine in the tracker -- the 1071 key-length gate is a storage-layer
/// concern, not a metadata one.
// go-parity-gap: no SchemaTracker carrier.
#[test]
#[ignore]
fn schema_tracker_allows_index_prefixes_beyond_the_key_length_limit() {
}

/// Go `TestSchemaTrackerAddPartitionRebuildsStorageClass`
/// (`pkg/ddl/schematracker/dm_tracker_test.go:132`): after ADD PARTITION on
/// a table whose ENGINE_ATTRIBUTE carries a storage-class tier with a
/// partition-scope predicate (`less_than` / `values_in`), the NEW
/// partition's `StorageClassTier` is IA when its bound falls inside the
/// declared scope and STANDARD when outside, for both RANGE and LIST
// partitioning; SHOW CREATE TABLE propagates an invalid storage-class
/// engine attribute as an error.
// go-parity-gap: no SchemaTracker carrier and no storage-class
// engine-attribute rebuild logic in this tier.
#[test]
#[ignore]
fn schema_tracker_add_partition_rebuilds_storage_class_tier() {
}

/// Go `TestExpressionIndexHiddenColumnState`
/// (`pkg/ddl/schematracker/dm_tracker_test.go:212`): creating a UNIQUE
/// expression index on `(lower(name))` -- via CREATE TABLE, CREATE INDEX or
/// ALTER TABLE ADD INDEX -- materializes a hidden column that is
/// StatePublic with `Hidden: true` and the expression attached, through
/// `requireExpressionIndexHiddenColumnsPublic`.
// go-parity-gap: no SchemaTracker carrier (the executor's own expression
// index path does not expose the tracker's hidden-column state contract).
#[test]
#[ignore]
fn schema_tracker_expression_index_hidden_columns_are_public() {
}

/// Go `TestAlterPK` (`pkg/ddl/schematracker/dm_tracker_test.go:253`): DROP
/// PRIMARY KEY removes the PK index from the tracker's metadata, ADD
/// PRIMARY KEY restores it, a second DROP removes it again -- and the
/// previously fetched `*model.TableInfo` stays immutable across all of it
/// (the tracker returns copies, never live pointers).
// go-parity-gap: no SchemaTracker carrier.
#[test]
#[ignore]
fn schema_tracker_alter_pk_leaves_old_table_info_immutable() {
}

/// Go `TestDropColumn` (`pkg/ddl/schematracker/dm_tracker_test.go:281`):
/// dropping column `b` (which carries the table's only index) removes the
/// index with the column; adding a two-column index over (a, c) and then
/// dropping `c` keeps the index (rebuilt over the remaining column) and
/// leaves exactly one column.
// go-parity-gap: no SchemaTracker carrier.
#[test]
#[ignore]
fn schema_tracker_drop_column_drops_its_index() {
}

/// Go `TestIndexLength` (`pkg/ddl/schematracker/dm_tracker_test.go:325`):
/// prefixed indexes on text/blob columns -- `a(768)`, `b(3072)`,
/// `c(3072)` -- round-trip through SHOW CREATE TABLE output identical to
/// the db_integration copy, whether the indexes came from CREATE TABLE or
/// from three ALTER TABLE ADD INDEX statements, and DeleteTable clears the
/// name for reuse.
// go-parity-gap: no SchemaTracker carrier and no
// ConstructResultOfShowCreateTable text renderer in this tier.
#[test]
#[ignore]
fn schema_tracker_index_length_round_trips_show_create_text() {
}

/// Go `TestCreateTableWithIndex`
/// (`pkg/ddl/schematracker/dm_tracker_test.go:362`): a JSON multi-valued
/// key (`KEY idx_1 ((cast(col_1 as char(64) array)))`) survives index
/// rename and re-add with its SHOW CREATE TABLE text intact.
// go-parity-gap: no SchemaTracker carrier; multi-valued index cast text is
// not carried either.
#[test]
#[ignore]
fn schema_tracker_keeps_multi_valued_index_text_across_rename() {
}

/// Go `TestIssue5092` (`pkg/ddl/schematracker/dm_tracker_test.go:383`): a
/// long ALTER TABLE ADD COLUMN chain -- parenthesized lists, IF NOT EXISTS
/// lists (silently skipping existing names), AFTER/FIRST positioning mixed
/// in one statement -- lands every column in the exact SHOW CREATE TABLE
/// order c2,a,b,d,e,g,h,f,c1,ff,b1...
// go-parity-gap: no SchemaTracker carrier; the multi-action ADD COLUMN
// ordering contract is not exercised anywhere else in this tier.
#[test]
#[ignore]
fn schema_tracker_add_column_chain_keeps_show_create_order() {
}

/// Go `TestBitDefaultValues` (`pkg/ddl/schematracker/dm_tracker_test.go:424`):
/// a table of every type family declared `NULL DEFAULT NULL` builds, and a
/// BIT column with an explicit binary default round-trips the default
/// through SHOW CREATE TABLE output byte-exactly.
// go-parity-gap: no SchemaTracker carrier and no SHOW CREATE renderer.
#[test]
#[ignore]
fn schema_tracker_bit_and_null_default_values_round_trip() {
}

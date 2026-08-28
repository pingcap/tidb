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

//! Ports of the `pkg/ddl/storage_class*` (master) slice owned by this batch.
//!
//! Carrier status, precisely: the workspace crate `tidb-exec` carries a
//! COMPLETE port of `pkg/ddl/storage_class.go` (`rust/crates/tidb-exec/src/
//! storage_class.rs`, "21 functions... plus the four named partition.go
//! helpers"), and its in-crate tests already pin the seven pure-function Go
//! tests of this slice:
//!
//! | Go test (master) | tidb-exec in-crate test |
//! | --- | --- |
//! | `pkg/ddl/storage_class_test.go:25::TestBuildStorageClassSettingsFromJSON` | `build_storage_class_settings_from_json_cases` (`src/storage_class.rs:1130`) |
//! | `pkg/ddl/storage_class_test.go:250::TestBuildStorageClassForTable` | `build_storage_class_for_table_cases` (`src/storage_class.rs:1211`) |
//! | `pkg/ddl/storage_class_test.go:304::TestBuildStorageClassForPartitions` | `build_storage_class_for_partitions_cases` (`src/storage_class.rs:1331`) |
//! | `pkg/ddl/storage_class_test.go:674::TestStorageClassString` | `storage_class_string_cases` (`src/storage_class.rs:1550`) |
//! | `pkg/ddl/storage_class_test.go:714::TestGetEngineAttributeFromStorageClassTableOptions` | `get_engine_attribute_from_storage_class_table_options_cases` (`src/storage_class.rs:1587`) |
//! | `pkg/ddl/storage_class_test.go:811::TestCheckStorageClassConflictInAlterTableSpecs` | `check_storage_class_conflict_in_alter_table_specs_cases` (`src/storage_class.rs:1640`) |
//! | `pkg/ddl/storage_class_test.go:869::TestGetSimpleTableStorageClassForShowCreate` | `get_simple_table_storage_class_for_show_create_cases` (`src/storage_class.rs:1671`) |
//!
//! `tidb-exec` is not a dependency of this gate crate, and the five tests
//! below need the CREATE-TABLE metadata-build path or `pkg/ddl/partition.go`'s
//! reorg helpers, which no crate carries, so they are recorded as `#[ignore]`
//! gaps with the contracts re-derived from the Go source. Nothing is
//! approximated.

/// Go `TestStorageClassPartitionScopesUseNormalizedValues`
/// (`pkg/ddl/storage_class_test.go:567`): through the full CREATE TABLE
/// build (`BuildTableInfoFromAST`), a `storage_class` scope inside
/// `ENGINE_ATTRIBUTE` binds partition tiers by NORMALIZED bounds:
/// `less_than: "200"` covers a `values less than (100 + 100)` expression
/// partition (tier IA) but not a 300 one; unsigned bigint bounds compare as
/// numbers up to 18446744073709551614; `range columns` on int, datetime
/// (string-compare with quoted bounds), and varchar-with-collation columns
/// match their normalized bound text; a `values_in: ["2"]` LIST scope
/// matches an `in (1 + 1)` expression partition; and a `MAXVALUE` scope
/// matches both the quoted `'MAXVALUE'` and keyword spellings. Each
/// partition's stored `LessThan`/`InValues` keeps the normalized text.
// go-parity-gap: `BuildTableInfoFromAST` (the CREATE TABLE metadata builder
// that binds storage-class scopes at create time) is not transcreated; the
// pure scope-matching halves it calls ARE carried in tidb-exec
// (`src/storage_class.rs`).
#[test]
#[ignore]
fn storage_class_partition_scopes_bind_normalized_bounds_at_create() {
}

/// Go `TestStorageClassPartitionScopesRejectInvalidLessThanValue`
/// (`pkg/ddl/storage_class_test.go:652`): `ENGINE_ATTRIBUTE` with
/// `{"tier":"IA", "less_than":"abc"}` over a RANGE(id) table fails the
/// CREATE TABLE build with `invalid 'less_than' value` -- the scope bound
/// must parse against the partition column's type.
// go-parity-gap: same missing `BuildTableInfoFromAST` create-path carrier.
#[test]
#[ignore]
fn storage_class_scope_rejects_a_non_numeric_less_than_bound() {
}

/// Go `TestStorageClassAddPartitionUsesCheckedDefinitions`
/// (`pkg/ddl/storage_class_partition_test.go:28`): ADD PARTITION tiers the
/// new definition through the CHECKED-defs path -- a range table scoped
/// `less_than: "300"` tiers an added `values less than (100 + 200)`
/// partition IA (expression folded to the scope bound), a list table scoped
/// `values_in: ["4"]` tiers an added `values in (2 + 2)` partition IA, and
/// an ALTER on a table whose ENGINE_ATTRIBUTE carries no storage_class
/// settings keeps every existing tier (`onAlterTableStorageClassSettings`
/// re-derives from the table's tiers, not from a settings document).
// go-parity-gap: `BuildAddedPartitionInfo`,
// `checkPartitionDefinitionConstraints` and
// `updatePartInfoDefinitionsFromFinalDefinitions` (`pkg/ddl/partition.go`)
// are not transcreated.
#[test]
#[ignore]
fn add_partition_tiers_new_definitions_through_the_checked_path() {
}

/// Go `TestStorageClassReorganizePartitionUsesCheckedDefinitions`
/// (`pkg/ddl/storage_class_partition_test.go:99`): REORGANIZE p1 INTO two
/// partitions on a table scoped `less_than: "200"` lands the split parts
/// tiered by their folded bounds -- the `100 + 100` part IA (bound 200) and
/// the 300 part STANDARD -- after `getReplacedPartitionIDs` +
/// `checkReorgPartitionDefs` run.
// go-parity-gap: `checkReorgPartitionDefs`/`getReplacedPartitionIDs`
// (`pkg/ddl/partition.go`) are not transcreated.
#[test]
#[ignore]
fn reorganize_partition_tiers_the_split_definitions() {
}

/// Go `TestStorageClassRemovePartitioningIgnoresPartitionScopes`
/// (`pkg/ddl/storage_class_partition_test.go:124`): removing partitioning on
/// a table whose ENGINE_ATTRIBUTE scopes tiers per partition builds the
/// single collapsed definition WITHOUT error -- partition-scoped settings
/// are ignored when the table stops being partitioned
/// (`checkReorgPartitionDefs` with `ActionRemovePartitioning`).
// go-parity-gap: same missing `pkg/ddl/partition.go` reorg carrier.
#[test]
#[ignore]
fn remove_partitioning_ignores_partition_scoped_settings() {
}

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

//! `pkg/planner.part14` DOCUMENTED GAP ports for
//! `pkg/planner/core/plan_to_pb_test.go` (items :30 and :101).
//!
//! The tested surface is `util.ColumnToProto`/`util.ColumnsToProto`
//! (`pkg/util/misc.go:322/339`) over `model.ColumnInfo` plus the
//! `tables.BuildPartitionTableScanFromInfos` generated-column propagation
//! (`pkg/table/tables/tables.go:1866`). This gate crate has no
//! `model.ColumnInfo → tipb.ColumnInfo` converter: the nearest Rust shape is
//! `tidb-exec`'s PRIVATE region-scan column descriptor
//! (`crates/tidb-exec/src/cop_scan.rs`, family-limited, no
//! generated-column/TiFlash-store arm and no global new-collation toggle),
//! which cannot express these contracts. Both items are honest
//! `#[ignore]` gap ports.

/// GO PARITY GAP port of `pkg/planner/core/plan_to_pb_test.go:30
/// TestColumnToProto`.
///
/// go-parity-gap: `util.ColumnToProto` (pkg/util/misc.go:339) unported. Go
/// pins, with new collation DISABLED: a `TypeLong` column with flag 10 and
/// `utf8_bin` encodes to `{Tp: 3, Collation: 83, ColumnLen: 11, Flag: 10}`;
/// `ColumnsToProto` keeps the flag with and without `pkIsHandle`; a
/// `latin1_swedish_ci` varchar encodes collation 8. With new collation
/// ENABLED the same columns encode NEGATED collation ids (-83, -8) —
/// `RewriteNewCollationIDIfNeeded`. An enum column carries its `Elems`, and
/// an ARRAY varchar builds `{Tp: 0xfe, Collation: 63, ColumnLen: 100}` with
/// `forIndex=true`.
#[test]
#[ignore = "go-parity-gap: util.ColumnToProto/ColumnsToProto over model.ColumnInfo (misc.go:322/339) unported, incl. new-collation id negation"]
fn column_to_proto_pins_flag_collation_elems_and_array() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_to_pb_test.go:101
/// TestGeneratedColumnFlagForTiFlash` (regression for pingcap/tidb#59831).
///
/// go-parity-gap: same missing converter, plus the TiFlash-store flag arm.
/// Go pins that `GeneratedColumnFlag` is set on the proto ONLY for a VIRTUAL
/// generated column (`GeneratedStored == false`) AND `isTiFlashStore=true`
/// — never for normal columns or non-TiFlash encodes — through
/// `ColumnToProto` AND `ColumnsToProto(forIndex=false, isTiFlashStore=true)`,
/// and that `tables.BuildPartitionTableScanFromInfos`
/// (pkg/table/tables/tables.go:1866) propagates the same per-column flags.
#[test]
#[ignore = "go-parity-gap: ColumnToProto isTiFlashStore GeneratedColumnFlag arm + BuildPartitionTableScanFromInfos unported"]
fn generated_column_flag_set_only_for_tiflash_virtual_generated_columns() {}

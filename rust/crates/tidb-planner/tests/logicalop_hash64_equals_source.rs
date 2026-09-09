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

//! Port of
//! `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go`
//! (`pkg/planner.part12` items 708-720 on `origin/master`), which pins every
//! logical operator's GENERATED `Hash64`/`Equals` identity
//! (`pkg/planner/core/operator/logicalop/hash64_equals_generated.go`).
//!
//! Per-operator Rust surfaces: each Go assertion sequence ("build two equal
//! operators; mutate ONE field group at a time; hash/equality must flip and
//! flip back") is replayed against the matching normalized identity type this
//! crate ships for that operator (`logical_top_n::LogicalTopNIdentity`,
//! `logical_table_dual::LogicalTableDualIdentity`, ...) whose field order was
//! transcribed from the same generated Go bodies. Two identities always hash
//! through `tidb_planner::hash_equaler`'s FNV-1a primitive, so "hashes differ"
//! pins exactly one thing: the mutated field GROUP is covered by the operator's
//! generated hash — never an absolute digest.
//!
//! Deviations are per-test documented:
//! * Go columns are `&expression.Column{ID: n, Index: 0}` with a zero-valued
//!   `UniqueID`; the Rust adapters take `(id, unique_id, index)`.
//! * Go distinguishes nil vs empty slices via `base.NilFlag`/`NotNilFlag`
//!   markers; Rust's `Option<Vec<_>>` preserves both states where the tests
//!   need them (`LogicalSortIdentity`) and `Vec::new()` stands in for nil where
//!   the source field is a plain `Vec`
//!   (`logical::projection::LogicalProjection.exprs`, `conditions`).
//! * The two no-attribute operators (`LogicalSequence`,
//!   `LogicalMaxOneRow`) hash their embedded BaseLogicalPlan ID, which in Go is
//!   allocated from the SESSION-global counter — so two fresh `Init` calls
//!   yield DIFFERENT ids and therefore different hashes
//!   (`hash64_equals_test.go:246-264`, `:510-528`). The Rust identities take
//!   the id explicitly; the tests hand them distinct values for "two Inits"
//!   and align them afterwards, mirroring `SetID(m1.ID())`.

use tidb_planner::logical::projection::LogicalProjection;
use tidb_planner::logical::{schema_producer, BaseLogicalPlan};
use tidb_planner::logical_limit::{LimitColumnIdentity, LimitSortItem, LogicalLimitIdentity};
use tidb_planner::logical_max_one_row::LogicalMaxOneRowIdentity;
use tidb_planner::logical_mem_table::{LogicalMemTableIdentity, MemTableColumnIdentity};
use tidb_planner::logical_sequence::LogicalSequenceIdentity;
use tidb_planner::logical_show::LogicalShowIdentity;
use tidb_planner::logical_show_ddl_jobs::{LogicalShowDDLJobsIdentity, ShowDDLJobsColumnIdentity};
use tidb_planner::logical_sort::{LogicalSortIdentity, SortByItem, SortColumnIdentity};
use tidb_planner::logical_table_dual::{ColumnIdentity, LogicalTableDualIdentity};
use tidb_planner::logical_top_n::{
    LogicalTopNIdentity, TopNByItem, TopNColumnIdentity, TopNSortItem,
};
use tidb_planner::logical_union_all::{LogicalUnionAllIdentity, UnionColumnIdentity};

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:36
/// TestLogicalTopNHash64Equals`.
///
/// Sequence re-derived from the source: `ByItems` column (:48-52), `ByItems`
/// direction (:53-58), `PartitionBy` direction (:59-64), `PartitionBy` column
/// (:65-70), `Offset` (:71-76), `Count` (:77-82), and `PreferLimitToCop`
/// (:83-88) each independently flip hash AND equality; restoring all fields
/// restores both (:89-93). Generated field order:
/// `hash64_equals_generated.go:856-883`.
#[test]
fn topn_hash64_equals_covers_by_items_partition_offset_count_and_cop_flag() {
    // p1 := LogicalTopN{}.Init(ctx, 1); ByItems=[{col1,true}];
    // PartitionBy=[{col1,true}]; Offset=Count=0; PreferLimitToCop=false.
    let top_n = |by_col: i64, by_desc: bool, part_col: i64, part_desc: bool,
                 offset: u64, count: u64, prefer: bool| {
        LogicalTopNIdentity::new(
            None,
            Some(vec![TopNByItem::new(TopNColumnIdentity::new(by_col, 0, 0), by_desc)]),
            Some(vec![TopNSortItem::new(Some(TopNColumnIdentity::new(part_col, 0, 0)), part_desc)]),
            offset,
            count,
            prefer,
        )
    };
    let p1 = top_n(1, true, 1, true, 0, 0, false);
    let p2 = top_n(1, true, 1, true, 0, 0, false);
    assert_eq!(p1.hash64(), p2.hash64());
    assert!(p1.equals(&p2));

    let p2 = top_n(2, true, 1, true, 0, 0, false);
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    let p2 = top_n(1, false, 1, true, 0, 0, false);
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    let p2 = top_n(1, true, 1, false, 0, 0, false);
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    let p2 = top_n(1, true, 2, true, 0, 0, false);
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    let p2 = top_n(1, true, 1, true, 2, 0, false);
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    let p2 = top_n(1, true, 1, true, 0, 1, false);
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    let p2 = top_n(1, true, 1, true, 0, 0, true);
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    let p2 = top_n(1, true, 1, true, 0, 0, false);
    assert_eq!(p1.hash64(), p2.hash64());
    assert!(p1.equals(&p2));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:114
/// TestLogicalTableDualHash64Equals`.
///
/// Two TableDuals over schema `[col1]` match (:126-136); swapping the schema to
/// `[col2]` breaks both halves (:137-142); `RowCount=2` breaks them too
/// (:143-149); restoring row count 0 restores parity (:150-156). Generated
/// body hashes tag + producer schema + RowCount
/// (`hash64_equals_generated.go:828-835`).
#[test]
fn table_dual_hash64_equals_tracks_schema_and_row_count() {
    let dual = |schema_id: i64, row_count: i64| {
        LogicalTableDualIdentity::new(
            Some(vec![ColumnIdentity::new(schema_id, 0, 0)]),
            row_count,
        )
    };
    let p1 = dual(1, 0);
    let p2 = dual(1, 0);
    assert_eq!(p1.hash64(), p2.hash64());
    assert!(p1.equals(&p2));

    let p2 = dual(2, 0);
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    let p2 = dual(1, 2);
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    let p2 = dual(1, 0);
    assert_eq!(p1.hash64(), p2.hash64());
    assert!(p1.equals(&p2));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:158
/// TestLogicalSortHash64Equals`.
///
/// This is the crate surface that also pins Go's NIL-vs-empty slice marker:
/// two default Sorts (nil ByItems) match (:168-176); an EMPTY non-nil ByItems
/// list differs from nil because `NotNilFlag`+len replaces `NilFlag`
/// (:177-182); one item differs again (:183-188); flipping only DESC keeps the
/// sorts apart in both directions (:189-200); aligning both to
/// `[{col1,desc}]` restores equality (:201-206). Generated body:
/// `hash64_equals_generated.go:791-806`.
#[test]
fn sort_hash64_equals_separates_nil_empty_and_directional_by_items() {
    let s1_none = LogicalSortIdentity::new(None);
    let s2_none = LogicalSortIdentity::new(None);
    assert_eq!(s1_none.hash64(), s2_none.hash64());
    assert!(s1_none.equals(&s2_none));

    let s2_empty = LogicalSortIdentity::new(Some(Vec::new()));
    assert_ne!(s1_none.hash64(), s2_empty.hash64());
    assert!(!s1_none.equals(&s2_empty));

    let col_asc = || SortColumnIdentity::new(1, 0, 0);
    let s2_one_desc = LogicalSortIdentity::new(Some(vec![SortByItem::new(col_asc(), true)]));
    assert_ne!(s1_none.hash64(), s2_one_desc.hash64());

    let s1_asc = LogicalSortIdentity::new(Some(vec![SortByItem::new(col_asc(), false)]));
    assert_ne!(s1_asc.hash64(), s2_one_desc.hash64());
    assert!(!s1_asc.equals(&s2_one_desc));

    let s1_desc = LogicalSortIdentity::new(Some(vec![SortByItem::new(col_asc(), true)]));
    assert_eq!(s1_desc.hash64(), s2_one_desc.hash64());
    assert!(s1_desc.equals(&s2_one_desc));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:199
/// TestLogicalShowDDLJobs`.
///
/// Two fresh ShowDDLJobs operators (nil producer schema) are identical
/// (:207-218); giving either one a schema `[col1]` breaks hash and equality
/// (:219-225) because the generated body is tag + LogicalSchemaProducer
/// (`hash64_equals_generated.go:767-774`). The untagged JobNumber field is
/// deliberately NOT hashed by Go's generator, so it never appears here either.
#[test]
fn show_ddl_jobs_hash64_equals_tracks_producer_schema() {
    let jobs_with_schema =
        |schema: Option<Vec<i64>>| {
            LogicalShowDDLJobsIdentity::new(schema.map(|ids| {
                ids.into_iter().map(|id| ShowDDLJobsColumnIdentity::new(id, 0, 0)).collect()
            }))
        };
    let s1 = jobs_with_schema(None);
    let s2 = jobs_with_schema(None);
    assert_eq!(s1.hash64(), s2.hash64());
    assert!(s1.equals(&s2));

    let s2 = jobs_with_schema(Some(vec![1]));
    assert_ne!(s1.hash64(), s2.hash64());
    assert!(!s1.equals(&s2));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:222
/// TestLogicalShowHash64Equals`.
///
/// Same contract as ShowDDLJobs but for LogicalShow's own plan tag: two
/// nil-schema shows match (:232-243), one carrying schema `[col1]` differs
/// (:244-249). Generated body: `hash64_equals_generated.go:743-750`.
#[test]
fn show_hash64_equals_tracks_producer_schema() {
    let show = |schema: Option<Vec<tidb_planner::logical_show::ShowColumnIdentity>>| {
        LogicalShowIdentity::new(schema)
    };
    let s1 = show(None);
    let s2 = show(None);
    assert_eq!(s1.hash64(), s2.hash64());
    assert!(s1.equals(&s2));

    let s2 = show(Some(vec![tidb_planner::logical_show::ShowColumnIdentity::new(1, 0, 0)]));
    assert_ne!(s1.hash64(), s2.hash64());
    assert!(!s1.equals(&s2));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:246
/// TestLogicalSequence`.
///
/// LogicalSequence has NO attributes of its own, so its identity is exactly
/// the embedded BaseLogicalPlan ID. Two independent inits in Go get different
/// session-global IDs, hence hashes DIFFER first (:256-263) and only
/// `m2.SetID(m1.ID())` aligns them (:264-268). Rust mirrors this with
/// explicit ids `1`/`2` standing in for what Go's allocator would produce.
/// Generated body: `hash64_equals_generated.go:719-726`.
#[test]
fn sequence_hash64_equals_pins_the_unique_plan_id() {
    let m1 = LogicalSequenceIdentity::new(1);
    let m2 = LogicalSequenceIdentity::new(2);
    assert_ne!(m1.hash64(), m2.hash64());
    assert!(!m1.equals(m2));

    let m2 = LogicalSequenceIdentity::new(m1.plan_id());
    assert_eq!(m1.hash64(), m2.hash64());
    assert!(m1.equals(m2));
}

/// GAP PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:266
/// TestLogicalSelectionHash64Equals`.
///
/// Go asserts: equal condition lists ([col1]) hash/compare equal; empty list
/// and nil both break hash+equality against [col1] (:278-303), and so does a
/// different column (:304-309). The exercised surface,
/// `LogicalSelection.Hash64/Equals` (`hash64_equals_generated.go:682-713`),
/// has NO Rust counterpart yet: `logical::selection::LogicalSelection` carries
/// `conditions: Vec<Expression>` but implements neither generated hashing nor
/// Equals, and no `SelectionIdentity` adapter exists. Adding one would be new
/// production code, out of test-port scope.
#[test]
#[ignore = "go-parity-gap: LogicalSelection has no generated Hash64/Equals implementation or identity adapter in the Rust crate"]
fn selection_hash64_equals_tracks_condition_lists() {}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:308
/// TestLogicalProjectionHash64Equals`.
///
/// Ports onto the REAL merged operator `logical::projection::LogicalProjection`
/// (`hash64(schema)` / `equals(...)`), the crate's home for the generated
/// projection body (`hash64_equals_generated.go:633-681`). Sequence: equal
/// expr lists + equal schemas match (:320-330); EMPTY exprs break both
/// (:331-337) — Go then sets nil separately (:338-343), which the same
/// `Vec::new()` represents here because every list shape still differs from a
/// one-element list; `CalculateNoDelay` flips both halves (:344-355);
/// `Proj4Expand` flips them (:356-361) and resetting it restores parity
/// (:362-367). Unlike the sibling identity adapters this operator also feeds
/// the producer schema as a parameter, so the schema stays covered implicitly.
#[test]
fn projection_hash64_equals_tracks_exprs_flags_and_schema() {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::expression::Expression;
    use tidb_expr::schema::Schema;

    let column = |unique_id: i64| Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
    let output = Schema::new(vec![column(1)]);
    let projection = |expr_unique_id: i64| {
        LogicalProjection::new(BaseLogicalPlan::default(), vec![Expression::Column(column(expr_unique_id))])
    };

    let p1 = projection(2);
    let mut p2 = projection(2);
    assert_eq!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(p1.equals(Some(&output), &p2, Some(&output)));

    p2 = LogicalProjection::new(BaseLogicalPlan::default(), Vec::new());
    assert_ne!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(!p1.equals(Some(&output), &p2, Some(&output)));

    p2 = LogicalProjection::new(BaseLogicalPlan::default(), Vec::new());
    assert_ne!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(!p1.equals(Some(&output), &p2, Some(&output)));

    p2 = projection(2);
    p2.calculate_no_delay = true;
    assert_ne!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(!p1.equals(Some(&output), &p2, Some(&output)));

    p2.calculate_no_delay = false;
    p2.proj4_expand = true;
    assert_ne!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(!p1.equals(Some(&output), &p2, Some(&output)));

    p2.proj4_expand = false;
    assert_eq!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(p1.equals(Some(&output), &p2, Some(&output)));

    // (extra guard for the schema half of the identity: same exprs under a
    // different producer schema are a different operator)
    let other_output = Schema::new(vec![column(9)]);
    assert_ne!(p1.hash64(Some(&output)), p1.hash64(Some(&other_output)));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:368
/// TestLogicalUnionAllHash64Equals` — FIRST HALF (LogicalUnionAll).
///
/// Two UnionAll operators over schema `[col1]` match (:379-389); `[col2]`
/// breaks both halves (:390-395). Generated body: plan tag + producer
/// (`hash64_equals_generated.go:585-592`). The SECOND half of the Go test
/// (PartitionUnionAll, :396-476 of the file) is a separate gap port below.
#[test]
fn union_all_hash64_equals_tracks_producer_schema() {
    let union_all = |schema_ids: Option<Vec<i64>>| {
        LogicalUnionAllIdentity::new(schema_ids.map(|ids| {
            ids.into_iter()
                .map(|id| UnionColumnIdentity::new(id, 0, 0))
                .collect()
        }))
    };
    let u1 = union_all(Some(vec![1]));
    let u2 = union_all(Some(vec![1]));
    assert_eq!(u1.hash64(), u2.hash64());
    assert!(u1.equals(&u2));

    let u2 = union_all(Some(vec![2]));
    assert_ne!(u1.hash64(), u2.hash64());
    assert!(!u1.equals(&u2));
}

/// GAP PORT of the SECOND HALF of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:368
/// TestLogicalUnionAllHash64Equals` (file lines 408-476).
///
/// Go additionally proves `LogicalPartitionUnionAll` is its OWN identity: two
/// partition unions over `[col1]` hash equal (:420-430) and `[col2]` breaks
/// them (:431-436), with the distinct `TypePartitionUnion` tag layered over the
/// embedded UnionAll (`hash64_equals_generated.go:609-616`). No Rust surface
/// models that tag difference — the top-level
/// `logical_union_all::LogicalUnionAllIdentity` hard-codes `"Union"` and the
/// full `logical::union_all::LogicalPartitionUnionAll` operator implements no
/// Hash64/Equals.
#[test]
#[ignore = "go-parity-gap: LogicalPartitionUnionAll's TypePartitionUnion identity (tag over embedded UnionAll) is unported"]
fn partition_union_all_hash64_uses_its_own_plan_tag() {}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:416
/// TestLogicalMemTableHash64Equals`.
///
/// MemTables over schema `[col1]`, DBName "" and nil TableInfo match
/// (:428-438). Then each identity field flips in turn: schema `[col2]`
/// (:439-444); DBName "d1" — CIStr hashes lower-case, reproduced by the
/// identity's case-folding — (:445-451); non-nil TableInfo `{ID:1}` vs nil
/// (:452-457); non-nil-but-empty `{}` vs `{ID:1}`, i.e. ID=0 encoded as
/// `Some(0)` (:458-464); back to nil restores parity (:465-471). The closing
/// Go block (:472-477) hands m1 a table info and is the ONLY step whose
/// assertions target m1-vs-m2; note it feeds `m1.Hash64(hasher2)` after
/// resetting hasher1 while comparing hasher1-vs-hash2 (the reset hasher1 holds
/// just the offset basis), so the meaningful pin there is
/// `m1.Equals(m2)==false` plus a differing digest, which is reproduced here
/// with fresh digests. Generated body:
/// `hash64_equals_generated.go:548-561`.
#[test]
fn mem_table_hash64_equals_tracks_schema_db_name_and_table_info_id() {
    let mem_table = |schema_id: Option<i64>, db: &str, table_info_id: Option<i64>| {
        LogicalMemTableIdentity::new(
            schema_id.map(|id| vec![MemTableColumnIdentity::new(id, 0, 0)]),
            db,
            table_info_id,
        )
    };
    let m1 = mem_table(Some(1), "", None);
    let m2 = mem_table(Some(1), "", None);
    assert_eq!(m1.hash64(), m2.hash64());
    assert!(m1.equals(&m2));

    let m2 = mem_table(Some(2), "", None);
    assert_ne!(m1.hash64(), m2.hash64());
    assert!(!m1.equals(&m2));

    let m2 = mem_table(Some(1), "d1", None);
    assert_ne!(m1.hash64(), m2.hash64());
    assert!(!m1.equals(&m2));

    let m2 = mem_table(Some(1), "", Some(1));
    assert_ne!(m1.hash64(), m2.hash64());
    assert!(!m1.equals(&m2));

    // `&model.TableInfo{}`: present but zero-valued (ID 0).
    let m2 = mem_table(Some(1), "", Some(0));
    assert_ne!(m1.hash64(), m2.hash64());
    assert!(!m1.equals(&m2));

    let m2 = mem_table(Some(1), "", None);
    assert_eq!(m1.hash64(), m2.hash64());
    assert!(m1.equals(&m2));

    let m1_with_table = mem_table(Some(1), "", Some(1));
    assert_ne!(m1_with_table.hash64(), m2.hash64());
    assert!(!m1_with_table.equals(&m2));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:479
/// TestLogicalSchemaProducerHash64Equals`.
///
/// Go builds two DataSources whose only populated identity field is the
/// embedded LogicalSchemaProducer schema, shows they match over `[col1]`
/// (:491-501) and differ over `[col2]` (:502-507). The DataSource operator
/// itself is not yet hashed in Rust, but the exact sub-surface under test —
/// `LogicalSchemaProducer.Hash64/Equals`
/// (`logical_schema_producer.go:36`,`:51`, ported as
/// `logical::schema_producer::schema_hash64/schema_equals` over the REAL
/// `tidb_expr::Schema`) — is, so the contract is pinned at that layer.
#[test]
fn schema_producer_hash64_equals_over_real_schema_columns() {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::schema::Schema;

    let column = |unique_id: i64| Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
    let d1_left = Schema::new(vec![column(1)]);
    let d1_right = Schema::new(vec![column(1)]);
    assert_eq!(
        schema_producer::schema_hash64(Some(&d1_left)),
        schema_producer::schema_hash64(Some(&d1_right))
    );
    assert!(schema_producer::schema_equals(Some(&d1_left), Some(&d1_right)));

    let d2 = Schema::new(vec![column(2)]);
    assert_ne!(
        schema_producer::schema_hash64(Some(&d1_left)),
        schema_producer::schema_hash64(Some(&d2))
    );
    assert!(!schema_producer::schema_equals(Some(&d1_left), Some(&d2)));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:510
/// TestLogicalMaxOneRowHash64Equals`.
///
/// Same contract as Sequence (see above): attribute-less operator whose
/// identity is the session-allocated plan ID, so two fresh inits differ
/// (:520-527) until `SetID` aligns them (:528-533). Generated body:
/// `hash64_equals_generated.go:436-443`.
#[test]
fn max_one_row_hash64_equals_pins_the_unique_plan_id() {
    let m1 = LogicalMaxOneRowIdentity::new(1);
    let m2 = LogicalMaxOneRowIdentity::new(2);
    assert_ne!(m1.hash64(), m2.hash64());
    assert!(!m1.equals(m2));

    let m2 = LogicalMaxOneRowIdentity::new(m1.plan_id());
    assert_eq!(m1.hash64(), m2.hash64());
    assert!(m1.equals(m2));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:530
/// TestLogicalLimitHash64Equals`.
///
/// Note Go constructs these limits as RAW struct literals (no Init), so no
/// schema/id state participates beyond producer-nil. `PartitionBy` column
/// (:550-556), `Offset` (:557-563), and `Count` (:564-570) each flip hash and
/// equality; the fully restored twin matches again (:571-577). Generated
/// order: tag + producer + PartitionBy + Offset + Count
/// (`hash64_equals_generated.go:387-404`).
#[test]
fn limit_hash64_equals_tracks_partition_by_offset_and_count() {
    let limit = |partition_col: i64, offset: u64, count: u64| {
        LogicalLimitIdentity::new(
            None,
            Some(vec![LimitSortItem::new(Some(LimitColumnIdentity::new(partition_col, 0, 0)), true)]),
            offset,
            count,
        )
    };
    let l1 = limit(1, 1, 1);
    let l2 = limit(1, 1, 1);
    assert_eq!(l1.hash64(), l2.hash64());
    assert!(l1.equals(&l2));

    let l2 = limit(2, 1, 1);
    assert_ne!(l1.hash64(), l2.hash64());
    assert!(!l1.equals(&l2));

    let l2 = limit(1, 2, 1);
    assert_ne!(l1.hash64(), l2.hash64());
    assert!(!l1.equals(&l2));

    let l2 = limit(1, 1, 2);
    assert_ne!(l1.hash64(), l2.hash64());
    assert!(!l1.equals(&l2));

    let l2 = limit(1, 1, 1);
    assert_eq!(l1.hash64(), l2.hash64());
    assert!(l1.equals(&l2));
}

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

//! Port of `pkg/planner.part13` items exercised against the HAND-WRITTEN
//! `Hash64`/`Equals` identities of `pkg/planner/core/operator/logicalop/
//! logical_window.go` (`FrameBound` :112/:148, `WindowFrame` :50/:62) and
//! `pkg/planner/util/handle_cols.go` (`CommonHandleCols` :107/:132), plus the
//! clone guarantees `logical_window.go:196-216` and
//! `logicalop_test/logical_operator_test.go:113` pin:
//!
//! * `hash64_equals_test.go:865 TestFrameBoundHash64Equals`
//! * `logical_operator_test.go:113 TestFrameBoundCloneDeepCopiesCompareCols`
//! * `hash64_equals_test.go:963 TestFrameBoundClonePreservesNilSlicesForHashEquals`
//! * `hash64_equals_test.go:985 TestWindowFrameHash64Equals`
//! * `hash64_equals_test.go:1025 TestHandleColsHash64Equals`
//!
//! Only equality RELATIONS are pinned, never absolute digests. The two Go
//! normalization seams are documented per test: `CmpFuncs` identity (Go hashes
//! `fmt.Sprintf("%p", f)`, `logical_window.go:139-142`) travels as string
//! tokens, and Go's nil-vs-empty slice framing collapses to empty/non-empty
//! `Vec`s where the tests need it.

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;

use tidb_planner::handle_cols::{CommonHandleIdentity, HandleColumnIdentity, HandleMetadataIdentity};
use tidb_planner::logical::schema_producer::expressions_equal;
use tidb_planner::logical::window::{BoundType, FrameBound, FrameType, RangeCmpDataType, WindowFrame};

/// Go `&expression.Column{UniqueID/ID, Index, RetType}`.
fn column(unique_id: i64, index: i64) -> Column {
    let mut col = Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
    col.id = unique_id;
    col.index = index;
    col
}

fn col_expr(unique_id: i64, index: i64) -> Expression {
    Expression::Column(column(unique_id, index))
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:865
/// TestFrameBoundHash64Equals`.
///
/// Sequence re-derived from the source: two fully-populated bounds match
/// (:897-900); mutating `Type` -> CurrentRow (:902-906), `UnBounded` ->
/// false (:908-913), `Num` -> 2 (:915-920), `CalcFuncs` -> [col2] (:922-927),
/// `CompareCols` -> [col2] (:929-934), `CmpFuncs` -> {MockFunc2}
/// (:936-941), `CmpDataType` (:949-953) or `IsExplicitRange` (:955-960)
/// each flip hash AND equality independently; each restoration restores both
/// (:943-947). Hand-written field order: `logical_window.go:112-146`.
#[test]
fn frame_bound_hash64_equals_tracks_bound_unbounded_num_funcs_and_range_flags() {
    // CmpFuncs identity: Go hashes the FUNCTION POINTER (`%p`),
    // logical_window.go:140. `MockFunc` vs `MockFunc2` are distinct addresses;
    // this port carries them as opaque tokens exactly one layer up.
    let bound = |calc_col: i64,
                 compare_col: i64,
                 cmp_token: &str,
                 cmp_data_type: RangeCmpDataType,
                 unbounded: bool,
                 num: u64,
                 bound_type: BoundType,
                 explicit_range: bool| {
        FrameBound {
            bound_type,
            unbounded,
            num,
            calc_funcs: vec![col_expr(calc_col, 0)],
            compare_cols: vec![col_expr(compare_col, 1)],
            cmp_func_tokens: vec![cmp_token.to_owned()],
            cmp_data_type,
            is_explicit_range: explicit_range,
        }
    };
    let base = || {
        bound(
            10,
            11,
            "mock_cmp_a",
            RangeCmpDataType::Int,
            true,
            1,
            BoundType::Preceding,
            false,
        )
    };

    let p1 = base();
    let p2 = base();
    assert_eq!(p1.hash64(), p2.hash64());
    assert!(p1.equals(&p2));

    // Type -> CurrentRow (:902-906).
    let p2 = bound(
        10,
        11,
        "mock_cmp_a",
        RangeCmpDataType::Int,
        true,
        1,
        BoundType::CurrentRow,
        false,
    );
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    // UnBounded -> false (:908-913).
    let p2 = bound(
        10,
        11,
        "mock_cmp_a",
        RangeCmpDataType::Int,
        false,
        1,
        BoundType::Preceding,
        false,
    );
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    // Num -> 2 (:915-920).
    let p2 = bound(
        10,
        11,
        "mock_cmp_a",
        RangeCmpDataType::Int,
        true,
        2,
        BoundType::Preceding,
        false,
    );
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    // CalcFuncs -> [col2]; col2's UniqueID=1 vs col's UniqueID=0 moves the
    // column hash code (:922-927).
    let p2 = bound(
        1,
        11,
        "mock_cmp_a",
        RangeCmpDataType::Int,
        true,
        1,
        BoundType::Preceding,
        false,
    );
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    // CompareCols -> [col2] (:929-934).
    let p2 = bound(
        10,
        12,
        "mock_cmp_a",
        RangeCmpDataType::Int,
        true,
        1,
        BoundType::Preceding,
        false,
    );
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    // CmpFuncs -> MockFunc2: a different function ADDRESS (:936-941).
    let p2 = bound(
        10,
        11,
        "mock_cmp_b",
        RangeCmpDataType::Int,
        true,
        1,
        BoundType::Preceding,
        false,
    );
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    // CmpFuncs restored — equality and digest come back together (:943-947).
    let p2 = bound(
        10,
        11,
        "mock_cmp_a",
        RangeCmpDataType::Int,
        true,
        1,
        BoundType::Preceding,
        false,
    );
    assert_eq!(p1.hash64(), p2.hash64());
    assert!(p1.equals(&p2));

    // CmpDataType changed to another wire code (:949-953). Go flips the raw
    // tipb code 1->2; the Rust variants carry names, so Int -> DateTime.
    let p2 = bound(
        10,
        11,
        "mock_cmp_a",
        RangeCmpDataType::DateTime,
        true,
        1,
        BoundType::Preceding,
        false,
    );
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));

    // IsExplicitRange -> true (:955-960).
    let p2 = bound(
        10,
        11,
        "mock_cmp_a",
        RangeCmpDataType::Int,
        true,
        1,
        BoundType::Preceding,
        true,
    );
    assert_ne!(p1.hash64(), p2.hash64());
    assert!(!p1.equals(&p2));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:963
/// TestFrameBoundClonePreservesNilSlicesForHashEquals`.
///
/// Go intent: `FrameBound.Clone` (`logical_window.go:196-216`) must keep the
/// ABSENT `CalcFuncs`/`CompareCols` lists absent while `CmpFuncs` stays
/// present, so original and clone still hash equal (:974-982).
///
/// DEVIATION: the Rust `FrameBound` carries plain `Vec`s, so "nil" is the
/// empty list; the pinned relation (clone preserves which lists are absent ->
/// identity preserved) still holds and is asserted.
#[test]
fn frame_bound_clone_keeps_absent_lists_absent_and_the_identity_unchanged() {
    // :965-972 — CalcFuncs/CompareCols absent, CmpFuncs present.
    let original = FrameBound {
        bound_type: BoundType::Preceding,
        unbounded: true,
        num: 1,
        calc_funcs: Vec::new(),
        compare_cols: Vec::new(),
        cmp_func_tokens: vec!["mock_cmp_a".to_owned()],
        cmp_data_type: RangeCmpDataType::Int,
        is_explicit_range: false,
    };
    // Go `original.Clone()`; derive(Clone) deep-copies owned content.
    let cloned = original.clone();

    // require.Nil(cloned.CalcFuncs/CompareCols), :974-975.
    assert!(cloned.calc_funcs.is_empty());
    assert!(cloned.compare_cols.is_empty());
    // ...while CmpFuncs survives cloning (Go shares the slice header :215).
    assert_eq!(cloned.cmp_func_tokens.len(), 1);

    // hasher1/hasher2 over original and clone (:976-981); only the two
    // digests' equality relation is pinnable — the crate exposes
    // FrameBound::hash64, not the bare HashEqualer.
    assert_eq!(original.hash64(), cloned.hash64());
    assert!(original.equals(&cloned));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logical_operator_test.go:113
/// TestFrameBoundCloneDeepCopiesCompareCols`.
///
/// Re-derived from the source: after `Clone`, replacing the CLONE's
/// `CompareCols[0]` must leave the ORIGINAL's `CompareCols[0]` AND its shared
/// `CalcFuncs[0]` untouched — the element lists are copies, not aliases
/// (`logical_window.go:203-214`: fresh slices, elements appended as clones).
#[test]
fn frame_bound_clone_deep_copies_compare_cols_against_replacement() {
    let original = FrameBound {
        calc_funcs: vec![col_expr(1, 0)],
        compare_cols: vec![col_expr(1, 0)],
        ..FrameBound::default()
    };
    let mut cloned = original.clone();

    // Len(cloned.CompareCols) == 1.
    assert_eq!(cloned.compare_cols.len(), 1);
    // Swap in an unrelated column on the CLONE only (Go replaces the element
    // with a UniqueID=2 column).
    let replaced = col_expr(2, 0);
    assert!(!expressions_equal(&replaced, &original.compare_cols[0]));
    cloned.compare_cols[0] = replaced.clone();
    assert!(expressions_equal(&cloned.compare_cols[0], &replaced));

    // The original keeps pointing at the first column, both lists.
    let first = col_expr(1, 0);
    assert!(expressions_equal(&original.compare_cols[0], &first));
    assert!(expressions_equal(&original.calc_funcs[0], &first));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:985
/// TestWindowFrameHash64Equals`.
///
/// Two frames with Type=Rows over identical bounds match (:1013-1016); moving
/// Type to the next code (:1018-1022; Go's raw 2 — parser ast.Ranges) flips
/// hash AND equality. Hand-written body: `WindowFrame.Hash64`
/// `logical_window.go:50-60`, including the quirk that only ONE bound ever
/// enters the digest when `Start` is set.
#[test]
fn window_frame_hash64_equals_tracks_frame_type_over_shared_bounds() {
    // Go uses start := end := the SAME *FrameBound; ownership here makes one
    // value per side, structurally identical — the relation under test.
    let make_frame = |frame_type| WindowFrame {
        frame_type,
        start: Some(FrameBound {
            bound_type: BoundType::Preceding,
            unbounded: true,
            num: 1,
            calc_funcs: vec![col_expr(7, 0)],
            compare_cols: vec![col_expr(7, 0)],
            cmp_func_tokens: vec!["mock_cmp_a".to_owned()],
            cmp_data_type: RangeCmpDataType::Int,
            is_explicit_range: false,
        }),
        end: None,
    };

    let w1 = make_frame(FrameType::Rows);
    let w2 = make_frame(FrameType::Rows);
    assert_eq!(w1.hash64(), w2.hash64());
    assert!(w1.equals(&w2));

    // Type 1 -> 2 (:1018-1022).
    let w2 = make_frame(FrameType::Ranges);
    assert_ne!(w1.hash64(), w2.hash64());
    assert!(!w1.equals(&w2));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:1025
/// TestHandleColsHash64Equals`.
///
/// Sequence re-derived from the source: two CommonHandleCols over
/// table{ID:1}/index{ID:1}/[col1,col2] match (:1037-1043); rebuilding over
/// table{ID:2} (:1041-1046), index{ID:2} (:1048-1053) or columns
/// [col2,col2] (:1055-1060) each flip hash AND equality. Source bodies:
/// `handle_cols.go:107-130` (Hash64: tblInfo, idxInfo, then the column list)
/// and `:132-158` (Equals).
#[test]
fn common_handle_cols_hash64_equals_tracks_table_index_and_column_identities() {
    let handles = |table_id: i64, index_id: i64, col_ids: [i64; 2]| {
        CommonHandleIdentity::new(
            Some(HandleMetadataIdentity::new(table_id)),
            Some(HandleMetadataIdentity::new(index_id)),
            Some(vec![
                HandleColumnIdentity::new(col_ids[0], col_ids[0], 0),
                HandleColumnIdentity::new(col_ids[1], col_ids[1], 1),
            ]),
        )
    };

    // handles1 / handles2 over t{1}, idx{1}, [u1, u2] (:1034-1043).
    let h1 = handles(1, 1, [1, 2]);
    let h2 = handles(1, 1, [1, 2]);
    assert_eq!(h1.hash64(), h2.hash64());
    assert!(h1.equals(&h2));

    // Table ID 2 (:1045-1046).
    let h2 = handles(2, 1, [1, 2]);
    assert_ne!(h1.hash64(), h2.hash64());
    assert!(!h1.equals(&h2));

    // Index ID 2 (:1048-1053); `NewCommonHandlesColsWithoutColsAlign`
    // (:330) supplies exactly these two metadata identities to fold.
    let h2 = handles(1, 2, [1, 2]);
    assert_ne!(h1.hash64(), h2.hash64());
    assert!(!h1.equals(&h2));

    // Columns swapped to [col2, col2]: unique ids change the element digests
    // (:1057-1060).
    let h2 = handles(1, 1, [2, 2]);
    assert_ne!(h1.hash64(), h2.hash64());
    assert!(!h1.equals(&h2));
}

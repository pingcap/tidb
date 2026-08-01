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

//! The wide SQL path's pushed predicates, lowered into coprocessor Selection
//! conditions and encoded into a DAG.

use prost::Message;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::EncodeType as DistSqlEncodeType;
use tidb_exec::dag_request::{
    construct_capped_read_only_dag_req_with_conditions, DagRequestContext, TiKvScanPlan,
};
use tidb_exec::wide_scan_selection::{
    accepts, wide_scan_selection_conditions, WideScanSelectionError,
};
use tidb_executor::predicate_pushdown::{ScanComparison, ScanComparisonOp, ScanPredicate};
use tidb_planner::{
    physical_table_scan::PhysicalTableScanPlan,
    tikv_scan_spec::{ScanColumnInfo, TiKvTableScanSpec},
};
use tidb_proto::tipb::{DagRequest, ExecType, Expr, ExprType, ScalarFuncSig};

/// Go `mysql.TypeLonglong`, `TypeLong`, `TypeVarchar`.
const MYSQL_TYPE_LONGLONG: i32 = 8;
const MYSQL_TYPE_LONG: i32 = 3;
const MYSQL_TYPE_VARCHAR: i32 = 15;
/// Go `mysql.UnsignedFlag`.
const UNSIGNED_FLAG: i32 = 32;

fn column_of(tp: i32, flags: i32) -> ScanColumnInfo {
    ScanColumnInfo {
        column_id: 1,
        tp,
        collation: 63,
        column_len: 20,
        decimal: 0,
        flag: flags,
        pk_handle: flags == 3,
        ..ScanColumnInfo::default()
    }
}

fn column_info(id: i64, flags: i32) -> ScanColumnInfo {
    ScanColumnInfo {
        column_id: id,
        ..column_of(MYSQL_TYPE_LONGLONG, flags)
    }
}

fn bigint() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn pushed(
    column_offset: u32,
    op: ScanComparisonOp,
    literal: Datum,
    column_on_left: bool,
) -> ScanPredicate {
    ScanPredicate::Compare(ScanComparison {
        column_offset,
        column_type: bigint(),
        op,
        literal,
        column_on_left,
    })
}

/// The pushed half of a wide-path `WHERE` really travels in the request: the
/// encoded DAG carries a Selection executor beside the scan, with one
/// condition per pushed conjunct.
#[test]
fn a_wide_path_pushed_predicate_is_carried_by_the_encoded_dag() {
    let columns = vec![column_info(1, 3), column_info(2, 1)];
    let table = PhysicalTableScanPlan::init(1, 0, TiKvTableScanSpec::new(42, columns.clone()));
    // `SELECT a, b FROM t WHERE a > 5 AND 10 > b`.
    let predicates = vec![
        pushed(0, ScanComparisonOp::Gt, Datum::Int(5), true),
        pushed(1, ScanComparisonOp::Gt, Datum::Int(10), false),
    ];
    let conditions = wide_scan_selection_conditions(&predicates, &columns).unwrap();
    let request = construct_capped_read_only_dag_req_with_conditions(
        &DagRequestContext::new("UTC", 0, 32, DistSqlEncodeType::Default),
        TiKvScanPlan::Table(&table),
        &conditions,
        None,
        &[0, 1],
    )
    .unwrap();

    // Decoding the bytes that would go on the wire, not the in-memory value.
    let decoded = DagRequest::decode(request.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.executors.len(), 2);
    assert_eq!(decoded.executors[0].tp, Some(ExecType::TypeTableScan as i32));
    let selection_executor = &decoded.executors[1];
    assert_eq!(selection_executor.tp, Some(ExecType::TypeSelection as i32));
    let conditions = &selection_executor.selection.as_ref().unwrap().conditions;
    assert_eq!(conditions.len(), 2, "one condition per pushed conjunct");

    // `a > 5`: the column stays on the left, as written.
    assert_eq!(conditions[0].sig, Some(ScalarFuncSig::GtInt as i32));
    assert_eq!(
        conditions[0].children[0].tp,
        Some(ExprType::ColumnRef as i32)
    );
    assert_eq!(conditions[0].children[1].tp, Some(ExprType::Int64 as i32));
    // `10 > b`: the literal stays on the left, so the operand order the
    // statement was written in survives into the request.
    assert_eq!(conditions[1].sig, Some(ScalarFuncSig::GtInt as i32));
    assert_eq!(conditions[1].children[0].tp, Some(ExprType::Int64 as i32));
    assert_eq!(
        conditions[1].children[1].tp,
        Some(ExprType::ColumnRef as i32)
    );
}

/// The whole integer column family lowers, because Go picks the same `*Int`
/// signature for all of it and inserts no cast: the column's declared type
/// travels on the `ColumnRef` leaf instead.
#[test]
fn every_integer_column_width_and_signedness_lowers_with_its_own_field_type() {
    for (tp, flags) in [
        (MYSQL_TYPE_LONGLONG, 0),
        (MYSQL_TYPE_LONGLONG, UNSIGNED_FLAG),
        (MYSQL_TYPE_LONG, 0),
        (MYSQL_TYPE_LONG, UNSIGNED_FLAG),
        (1, 0),  // TINYINT
        (2, 0),  // SMALLINT
        (9, 0),  // MEDIUMINT
        (13, 0), // YEAR
    ] {
        let columns = vec![column_of(tp, flags)];
        let condition = &wide_scan_selection_conditions(
            &[pushed(0, ScanComparisonOp::Gt, Datum::Int(5), true)],
            &columns,
        )
        .unwrap_or_else(|error| panic!("tp {tp} flags {flags} must lower: {error}"))[0];
        assert_eq!(condition.sig, Some(ScalarFuncSig::GtInt as i32));
        let column_type = condition.children[0].field_type.as_ref().unwrap();
        assert_eq!(
            (column_type.tp, column_type.flag),
            (Some(tp), Some(flags as u32)),
            "the column's own declared type reaches TiKV"
        );
    }
}

/// The refusals, each for a reason Go's own refinement makes necessary.
#[test]
fn the_wide_lowering_refuses_what_go_would_have_refined_or_cannot_compare_as_an_integer() {
    let signed = vec![column_of(MYSQL_TYPE_LONGLONG, 0)];
    let unsigned = vec![column_of(MYSQL_TYPE_LONGLONG, UNSIGNED_FLAG)];
    let text = vec![column_of(MYSQL_TYPE_VARCHAR, 0)];

    assert_eq!(
        wide_scan_selection_conditions(&[], &signed),
        Err(WideScanSelectionError::NoConditions)
    );
    // An INTEGER constant against a character-string column: the string path
    // owns this column, and Go rewrites the constant through
    // `RefineComparedConstant` rather than sending the comparison as written.
    assert_eq!(
        wide_scan_selection_conditions(
            &[pushed(0, ScanComparisonOp::Eq, Datum::Int(1), true)],
            &text
        ),
        Err(WideScanSelectionError::UnsupportedLiteral { offset: 0 })
    );
    // A non-integer constant: Go rewrites it through `RefineComparedConstant`.
    assert_eq!(
        wide_scan_selection_conditions(
            &[pushed(
                0,
                ScanComparisonOp::Eq,
                Datum::Bytes(b"x".to_vec()),
                true
            )],
            &signed
        ),
        Err(WideScanSelectionError::UnsupportedLiteral { offset: 0 })
    );
    // A non-positive constant against an UNSIGNED column: Go's
    // `refineArgsByUnsignedFlag` replaces the comparison with a known truth
    // value instead of sending it, so this lowering must not send it either.
    for value in [-1, 0] {
        assert_eq!(
            wide_scan_selection_conditions(
                &[pushed(0, ScanComparisonOp::Lt, Datum::Int(value), true)],
                &unsigned
            ),
            Err(WideScanSelectionError::UnsupportedLiteral { offset: 0 }),
            "unsigned column against {value}"
        );
    }
    // The same constant against a SIGNED column is an ordinary comparison.
    assert!(accepts(
        &pushed(0, ScanComparisonOp::Lt, Datum::Int(-1), true),
        &signed
    ));
    // An offset the scan does not have fails closed rather than indexing out.
    assert_eq!(
        wide_scan_selection_conditions(
            &[pushed(7, ScanComparisonOp::Eq, Datum::Int(1), true)],
            &signed
        ),
        Err(WideScanSelectionError::ColumnOffsetOutOfRange {
            offset: 7,
            width: 1
        })
    );
}

/// A `VARCHAR` column in a named collation, as a scan descriptor states it:
/// the REWRITTEN protocol id, which is what `cop_scan::scan_column` puts there.
fn string_column(collation: &str) -> ScanColumnInfo {
    ScanColumnInfo {
        collation: tidb_datatype::collation_to_proto(collation),
        ..column_of(MYSQL_TYPE_VARCHAR, 0)
    }
}

fn string_compare(op: ScanComparisonOp, literal: &[u8], column_on_left: bool) -> ScanPredicate {
    ScanPredicate::Compare(ScanComparison {
        column_offset: 0,
        column_type: FieldType::new(FieldTypeCode::Varchar),
        op,
        literal: Datum::Bytes(literal.to_vec()),
        column_on_left,
    })
}

/// A string comparison travels, as the `*String` signature Go's
/// `generateCmpSigs` picks for an `ETString` comparison.
///
/// The scan descriptor already carried the column itself faithfully -- type,
/// `flen` and the rewritten collation id (Go `util.ColumnToProto`) -- so what
/// this adds is the comparison over it. Go pushes all six ordinary comparison
/// operators to TiKV (`infer_pushdown.go`'s `scalarExprSupportedByTiKV`), and
/// the constant's bytes travel verbatim with no codec around them:
/// `constantToPBExpr` sends a `KindString` datum as `ExprType_String` with
/// `d.GetBytes()`, unlike the integer leaf's `codec.EncodeInt`.
#[test]
fn a_string_comparison_lowers_to_the_string_signature_go_resolves() {
    let text = vec![string_column("utf8mb4_bin")];
    // A multi-byte value: a byte-length or transcoding error is visible here
    // and invisible in an ASCII fixture.
    let literal = "a\u{e9}b".as_bytes();
    assert_eq!(literal.len(), 4, "the fixture really is multi-byte");

    for (op, expected) in [
        (ScanComparisonOp::Eq, ScalarFuncSig::EqString),
        (ScanComparisonOp::Ne, ScalarFuncSig::NeString),
        (ScanComparisonOp::Lt, ScalarFuncSig::LtString),
        (ScanComparisonOp::Le, ScalarFuncSig::LeString),
        (ScanComparisonOp::Gt, ScalarFuncSig::GtString),
        (ScanComparisonOp::Ge, ScalarFuncSig::GeString),
    ] {
        let predicate = string_compare(op, literal, true);
        assert!(accepts(&predicate, &text), "{op:?} must travel");
        let condition = wide_scan_selection_conditions(&[predicate], &text)
            .unwrap()
            .remove(0);
        assert_eq!(condition.sig, Some(expected as i32));
        assert_eq!(
            condition.children[0].tp,
            Some(ExprType::ColumnRef as i32),
            "the column stays on the left, as written"
        );
        let constant = &condition.children[1];
        assert_eq!(constant.tp, Some(ExprType::String as i32));
        assert_eq!(
            constant.val.as_deref(),
            Some(literal),
            "the constant's bytes travel verbatim, with no codec around them"
        );
    }

    // The flipped spelling keeps its operand order, as the integer path does.
    let flipped = wide_scan_selection_conditions(
        &[string_compare(ScanComparisonOp::Gt, literal, false)],
        &text,
    )
    .unwrap()
    .remove(0);
    assert_eq!(flipped.sig, Some(ScalarFuncSig::GtString as i32));
    assert_eq!(flipped.children[0].tp, Some(ExprType::String as i32));
    assert_eq!(flipped.children[1].tp, Some(ExprType::ColumnRef as i32));

    // And it all survives the encoding that actually goes on the wire.
    let scan = PhysicalTableScanPlan::init(0, 0, TiKvTableScanSpec::new(7, text.clone()));
    let conditions =
        wide_scan_selection_conditions(&[string_compare(ScanComparisonOp::Eq, literal, true)], &text)
            .unwrap();
    let request = construct_capped_read_only_dag_req_with_conditions(
        &DagRequestContext::new("UTC", 0, 0, DistSqlEncodeType::Default),
        TiKvScanPlan::Table(&scan),
        &conditions,
        None,
        &[0],
    )
    .unwrap();
    let decoded = DagRequest::decode(request.encode_to_vec().as_slice()).unwrap();
    let sent = &decoded.executors[1].selection.as_ref().unwrap().conditions[0];
    assert_eq!(sent.sig, Some(ScalarFuncSig::EqString as i32));
    assert_eq!(sent.children[1].val.as_deref(), Some(literal));
}

/// The collation on the comparison node is the one TiKV compares with, and it
/// is the column's own.
///
/// TiKV reads the collator off the comparison node's OWN field type, not off
/// either operand's: Go writes it there in `scalarFuncToPBExpr`
/// (`tp := *expr.RetType; tp.SetCollate(str1)`), leaving the return type
/// `BIGINT(1)` and overwriting only its collation. A wrong id there is a
/// silently wrong answer computed at the region, so this asserts the exact id
/// -- and then asserts what that id MEANS, by asking the same collator table
/// TiKV mirrors whether `'A' = 'a'`. The case-insensitive and the binary
/// fixtures must disagree about that, or the fixture could not tell a right
/// collation from a wrong one.
#[test]
fn the_comparison_node_carries_the_columns_collation_and_that_collation_decides_case() {
    for (name, case_insensitive) in [
        ("utf8mb4_general_ci", true),
        ("utf8mb4_unicode_ci", true),
        ("utf8mb4_bin", false),
        ("binary", false),
    ] {
        let columns = vec![string_column(name)];
        let condition = wide_scan_selection_conditions(
            &[string_compare(ScanComparisonOp::Eq, b"a", true)],
            &columns,
        )
        .unwrap()
        .remove(0);
        let node_type = condition.field_type.as_ref().unwrap();
        assert_eq!(
            node_type.collate,
            Some(tidb_datatype::collation_to_proto(name)),
            "{name}: the comparison node carries the column's own collation"
        );
        assert_eq!(node_type.tp, Some(MYSQL_TYPE_LONGLONG), "{name}: BIGINT(1)");
        assert_eq!(node_type.flen, Some(1));
        // The column leaf agrees with it, so the collator TiKV compares with
        // and the collation it was told to read the column with are one.
        assert_eq!(
            condition.children[0].field_type.as_ref().unwrap().collate,
            node_type.collate
        );

        // What that id MEANS, asked of the collator table itself.
        let collator = tidb_datatype::get_collator_by_id(
            tidb_datatype::restore_collation_id_if_needed(node_type.collate.unwrap()),
        );
        assert_eq!(
            collator.compare(b"A", b"a") == std::cmp::Ordering::Equal,
            case_insensitive,
            "{name}: 'A' = 'a' must be {case_insensitive}"
        );
    }
}

/// The string refusals, each pinned so it flips rather than vanishes when the
/// gap closes.
#[test]
fn the_string_lowering_refuses_every_comparison_whose_collation_it_cannot_derive() {
    // A charset outside `utf8mb4`/`binary`: Go's `inferCollation` applies
    // repertoire rules this lowering does not implement, and a literal that
    // does not fit the column's repertoire changes the derived answer.
    for name in ["latin1_bin", "ascii_bin", "gbk_bin"] {
        let refused = wide_scan_selection_conditions(
            &[string_compare(ScanComparisonOp::Eq, b"a", true)],
            &[string_column(name)],
        );
        assert!(
            matches!(refused, Err(WideScanSelectionError::Expression(_))),
            "{name}: an underivable charset is refused, got {refused:?}"
        );
    }
    // A string constant against an INTEGER column stays the integer path's
    // own refusal, unchanged: Go refines that comparison too.
    assert_eq!(
        wide_scan_selection_conditions(
            &[pushed(
                0,
                ScanComparisonOp::Eq,
                Datum::Bytes(b"x".to_vec()),
                true
            )],
            &[column_of(MYSQL_TYPE_LONGLONG, 0)]
        ),
        Err(WideScanSelectionError::UnsupportedLiteral { offset: 0 })
    );
    // `ENUM` and `SET` evaluate as `ETString` in Go but are not in this
    // lowering's string family: their leaf needs the `elems` list on the wire
    // and Go gates them behind `IsPushDownEnabled`. They stay refused as
    // non-integer columns.
    for tp in [247, 248] {
        assert_eq!(
            wide_scan_selection_conditions(
                &[string_compare(ScanComparisonOp::Eq, b"a", true)],
                &[column_of(tp, 0)]
            ),
            Err(WideScanSelectionError::UnsupportedColumnType { offset: 0 }),
            "tp {tp}: ENUM/SET are refused"
        );
    }
    // `IN` and `IS NULL` over a string column are still the integer path's,
    // so they stay refused -- the widening here is the comparison only.
    assert_eq!(
        wide_scan_selection_conditions(
            &[ScanPredicate::In {
                column_offset: 0,
                column_type: FieldType::new(FieldTypeCode::Varchar),
                literals: vec![Datum::Bytes(b"a".to_vec())],
                negated: false,
            }],
            &[string_column("utf8mb4_bin")]
        ),
        Err(WideScanSelectionError::UnsupportedColumnType { offset: 0 })
    );
    assert_eq!(
        wide_scan_selection_conditions(
            &[ScanPredicate::IsNull {
                column_offset: 0,
                column_type: FieldType::new(FieldTypeCode::Varchar),
                negated: false,
            }],
            &[string_column("utf8mb4_bin")]
        ),
        Err(WideScanSelectionError::UnsupportedColumnType { offset: 0 })
    );
}

/// `IS NULL`, `IS NOT NULL`, `IN`, `NOT IN`, `OR` and `NOT`, each lowered to
/// the signature Go sends for it. `IS NOT NULL` and `NOT IN` have no
/// signature of their own: Go's rewriter spells them as `UnaryNot` over the
/// positive form, and so does this.
#[test]
fn the_composed_integer_predicates_lower_to_gos_own_signatures() {
    let columns = vec![column_of(MYSQL_TYPE_LONGLONG, 0)];
    let sig = |predicate: &ScanPredicate| -> Expr {
        wide_scan_selection_conditions(std::slice::from_ref(predicate), &columns)
            .unwrap()
            .remove(0)
    };

    let is_null = |negated| ScanPredicate::IsNull {
        column_offset: 0,
        column_type: bigint(),
        negated,
    };
    let positive = sig(&is_null(false));
    assert_eq!(positive.sig, Some(ScalarFuncSig::IntIsNull as i32));
    assert_eq!(positive.children.len(), 1);
    let negated = sig(&is_null(true));
    assert_eq!(negated.sig, Some(ScalarFuncSig::UnaryNotInt as i32));
    assert_eq!(
        negated.children[0].sig,
        Some(ScalarFuncSig::IntIsNull as i32)
    );

    let membership = |negated, literals: Vec<i64>| ScanPredicate::In {
        column_offset: 0,
        column_type: bigint(),
        literals: literals.into_iter().map(Datum::Int).collect(),
        negated,
    };
    let in_list = sig(&membership(false, vec![1, 2, 3]));
    assert_eq!(in_list.sig, Some(ScalarFuncSig::InInt as i32));
    assert_eq!(
        in_list.children.len(),
        4,
        "the tested column plus one child per list element"
    );
    // Go's `buildHashMapForConstArgs` removes duplicate constants before the
    // expression is converted, so they never reach the wire.
    assert_eq!(sig(&membership(false, vec![1, 1, 2])).children.len(), 3);
    let not_in = sig(&membership(true, vec![4]));
    assert_eq!(not_in.sig, Some(ScalarFuncSig::UnaryNotInt as i32));
    assert_eq!(not_in.children[0].sig, Some(ScalarFuncSig::InInt as i32));

    // `a = 1 OR a = 2 OR a = 3`: TiKV's LogicalOr is binary, so the chain
    // folds left, as Go's left-associative parse produces.
    let disjunction = sig(&ScanPredicate::Or(vec![
        pushed(0, ScanComparisonOp::Eq, Datum::Int(1), true),
        pushed(0, ScanComparisonOp::Eq, Datum::Int(2), true),
        pushed(0, ScanComparisonOp::Eq, Datum::Int(3), true),
    ]));
    assert_eq!(disjunction.sig, Some(ScalarFuncSig::LogicalOr as i32));
    assert_eq!(
        disjunction.children[0].sig,
        Some(ScalarFuncSig::LogicalOr as i32)
    );
    assert_eq!(
        disjunction.children[1].sig,
        Some(ScalarFuncSig::EqInt as i32)
    );

    // A single-branch OR needs no node at all.
    let single = sig(&ScanPredicate::Or(vec![pushed(
        0,
        ScanComparisonOp::Eq,
        Datum::Int(1),
        true,
    )]));
    assert_eq!(single.sig, Some(ScalarFuncSig::EqInt as i32));

    // One refused branch refuses the whole disjunction: an OR cannot be
    // partially pushed, because dropping a branch narrows the predicate and
    // would lose rows the query selects.
    assert!(!accepts(
        &ScanPredicate::Or(vec![
            pushed(0, ScanComparisonOp::Eq, Datum::Int(1), true),
            pushed(0, ScanComparisonOp::Eq, Datum::Bytes(b"x".to_vec()), true),
        ]),
        &columns
    ));

    let negation = sig(&ScanPredicate::Not(Box::new(pushed(
        0,
        ScanComparisonOp::Eq,
        Datum::Int(9),
        true,
    ))));
    assert_eq!(negation.sig, Some(ScalarFuncSig::UnaryNotInt as i32));
    assert_eq!(negation.children[0].sig, Some(ScalarFuncSig::EqInt as i32));
}

/// The pushed row cap becomes a coprocessor `Limit` above the Selection, so
/// the rows past it never leave the region. Go builds the same executor list
/// for a `LIMIT` pushed into a TiKV reader.
#[test]
fn a_pushed_cap_becomes_a_limit_executor_above_the_selection() {
    let columns = vec![column_info(1, 3), column_info(2, 0)];
    let scan = PhysicalTableScanPlan::init(0, 0, TiKvTableScanSpec::new(114, columns.clone()));
    let conditions = wide_scan_selection_conditions(
        &[pushed(0, ScanComparisonOp::Gt, Datum::Int(195), true)],
        &columns,
    )
    .expect("an integer comparison lowers");
    let dag = construct_capped_read_only_dag_req_with_conditions(
        &DagRequestContext::new("UTC", 0, 0, DistSqlEncodeType::Default),
        TiKvScanPlan::Table(&scan),
        &conditions,
        Some(5),
        &[0, 1],
    )
    .expect("the capped request builds");

    let kinds: Vec<i32> = dag
        .executors
        .iter()
        .map(|executor| executor.tp.expect("every executor names its type"))
        .collect();
    assert_eq!(
        kinds,
        vec![
            ExecType::TypeTableScan as i32,
            ExecType::TypeSelection as i32,
            ExecType::TypeLimit as i32,
        ],
        "the cap is the last executor: it counts rows the Selection kept"
    );
    assert_eq!(
        dag.executors[2]
            .limit
            .as_ref()
            .and_then(|limit| limit.limit),
        Some(5)
    );

    // The encoded bytes are what TiKV reads, so the cap has to survive them.
    let decoded = DagRequest::decode(dag.encode_to_vec().as_slice()).expect("the DAG round-trips");
    assert_eq!(
        decoded.executors[2]
            .limit
            .as_ref()
            .and_then(|limit| limit.limit),
        Some(5)
    );

    // Without a cap the executor list is the pair it always was.
    let uncapped = construct_capped_read_only_dag_req_with_conditions(
        &DagRequestContext::new("UTC", 0, 0, DistSqlEncodeType::Default),
        TiKvScanPlan::Table(&scan),
        &conditions,
        None,
        &[0, 1],
    )
    .expect("the uncapped request builds");
    assert_eq!(uncapped.executors.len(), 2);

    // No condition at all builds no Selection, so the cap sits directly on
    // the scan -- which is only sound because the driver offers a cap solely
    // when nothing above the source filters.
    let unfiltered = construct_capped_read_only_dag_req_with_conditions(
        &DagRequestContext::new("UTC", 0, 0, DistSqlEncodeType::Default),
        TiKvScanPlan::Table(&scan),
        &[],
        Some(3),
        &[0, 1],
    )
    .expect("an unfiltered capped request builds");
    assert_eq!(
        unfiltered
            .executors
            .iter()
            .map(|executor| executor.tp.unwrap())
            .collect::<Vec<_>>(),
        vec![
            ExecType::TypeTableScan as i32,
            ExecType::TypeLimit as i32
        ]
    );
}

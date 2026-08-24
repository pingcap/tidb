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

//! The index strategy's own gate: the rows it produces, against the hash
//! strategy over the SAME data.
//!
//! # Why the hash strategy is the oracle here
//!
//! `crate::driver::index_join_decision::CHOOSER_IS_FAITHFUL` is `false`, so
//! no statement reaches the index strategy through SQL yet and no recorded
//! `.result` can pin it. What CAN pin it is the answer this tier already
//! proves against TiDB everywhere else: the hash join over the same two
//! relations. The two strategies share `emit_outer_row`, the `ON`
//! evaluation, and the outer-join padding, so a difference between them is
//! never "a different but equal answer" -- it is the lookup reading the wrong
//! rows.
//!
//! Every test below runs the SAME join twice, once each way, and requires the
//! two row lists to be equal ELEMENT BY ELEMENT: an index join preserves its
//! outer side's order, so the comparison is on sequences and not on sets.

use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::{Expression, ScalarFunction};
use tidb_expr::schema::Schema;

use crate::access_path::{IndexJoinLookupExec, LookupObject, LookupProbePart};
use crate::executor::{Executor, ExecutorMeta};
use crate::join::{IndexLookupPlan, JoinExec, JoinKind};
use crate::kv_table::{KvColumn, KvIndex, KvTable};
use crate::mem_table::MemTableSourceExec;

const INIT_CAP: usize = 32;
const CHUNK: usize = 1024;

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn column(name: &str, id: i64) -> KvColumn {
    KvColumn {
        name: name.to_owned(),
        id,
        field_type: long(),
        column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None,
    }
}

fn schema_of(width: usize) -> Schema {
    Schema::new(
        (0..width)
            .map(|i| {
                let mut column = Column::new((i + 1) as i64, long());
                column.index = i as i64;
                column
            })
            .collect(),
    )
}

/// `inner(a, b)` with an index on `b`, holding `rows`.
///
/// `b` is deliberately NOT unique: an outer key that matches several inner
/// rows is the case a probe that stopped at the first entry, or a lookup map
/// that kept one row per key, would answer wrongly.
fn inner_table(rows: &[(i64, i64)]) -> KvTable {
    let mut table = KvTable::new(91, vec![column("a", 1), column("b", 2)]);
    table
        .create_index_with_context(
            KvIndex {
                id: 1,
                name: "ib".to_owned(),
                comment: String::new(),
                unique: false,
                column_offsets: vec![1],
                prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
                visible: true,
                global: false,
                clustered_primary: false,
            },
            &crate::StmtContext::for_query(),
        )
        .unwrap();
    for (a, b) in rows {
        table
            .insert_row(&[Datum::Int(*a), Datum::Int(*b)], &tidb_expr::NoColumns)
            .unwrap();
    }
    table
}

/// `inner(a, b)` clustered by the composite primary key `(a, b)`.
fn common_handle_table(rows: &[(i64, i64)]) -> KvTable {
    let mut table = KvTable::new(94, vec![column("a", 1), column("b", 2)]);
    table.set_common_handle_offsets(vec![0, 1]);
    table
        .create_index_with_context(
            KvIndex {
                id: 1,
                name: "PRIMARY".to_owned(),
                comment: String::new(),
                unique: true,
                column_offsets: vec![0, 1],
                prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
                visible: true,
                global: false,
                clustered_primary: false,
            },
            &crate::StmtContext::for_query(),
        )
        .unwrap();
    for (a, b) in rows {
        table
            .insert_row(&[Datum::Int(*a), Datum::Int(*b)], &tidb_expr::NoColumns)
            .unwrap();
    }
    table
}

/// The DDL representation of a clustered common handle: the PRIMARY is the
/// table path itself and is therefore absent from the secondary-index list.
fn ddl_common_handle_table(rows: &[(i64, i64)]) -> KvTable {
    let mut table = KvTable::new(95, vec![column("a", 1), column("b", 2)]);
    table.set_common_handle_offsets(vec![0, 1]);
    for (a, b) in rows {
        table
            .insert_row(&[Datum::Int(*a), Datum::Int(*b)], &tidb_expr::NoColumns)
            .unwrap();
    }
    table
}

fn outer_source(rows: &[Vec<Datum>]) -> Box<dyn Executor> {
    Box::new(MemTableSourceExec::new(
        ExecutorMeta::new(
            schema_of(rows.first().map_or(1, Vec::len)),
            0,
            INIT_CAP,
            CHUNK,
        ),
        rows.to_vec(),
    ))
}

/// A whole-table scan of `table`, which is what the hash strategy reads.
fn scan_of(table: &KvTable, width: usize) -> Box<dyn Executor> {
    Box::new(crate::kv_table::TableScanExec::new_with_context(
        ExecutorMeta::new(schema_of(width), 0, INIT_CAP, CHUNK),
        table.clone(),
        crate::RowDecodeContext::for_test_query_utc(),
        crate::remote_scan::PushdownStatementContext::default(),
    ))
}

fn lookup_source(table: &KvTable, object: LookupObject, width: usize) -> IndexJoinLookupExec {
    IndexJoinLookupExec::new_with_context(
        ExecutorMeta::new(schema_of(width), 0, INIT_CAP, CHUNK),
        table.clone(),
        object,
        crate::RowDecodeContext::for_test_query_utc(),
    )
}

/// `outer.<outer_key> = inner.<inner_key>` over the joined row, with the
/// outer side on the LEFT.
fn equality(outer_key: usize, inner_key: usize, outer_width: usize) -> Expression {
    let column = |index: usize| {
        let mut column = Column::new(index as i64 + 1, long());
        column.index = index as i64;
        Expression::Column(column)
    };
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("eq"),
        long(),
        vec![column(outer_key), column(outer_width + inner_key)],
    ))
}

fn drain(exec: &mut dyn Executor, types: &[FieldType]) -> Vec<Vec<Datum>> {
    exec.open().unwrap();
    let mut out = Vec::new();
    let mut chunk = exec.new_chunk();
    loop {
        exec.next(&mut chunk).unwrap();
        if chunk.num_rows() == 0 {
            break;
        }
        for r in 0..chunk.num_rows() {
            out.push(
                types
                    .iter()
                    .enumerate()
                    .map(|(c, ft)| chunk.get_row(r).get_datum(c, ft))
                    .collect(),
            );
        }
    }
    exec.close().unwrap();
    out
}

/// Both strategies' answers for one join, in that order.
fn both_ways(
    kind: JoinKind,
    outer_rows: &[Vec<Datum>],
    table: &KvTable,
    object: LookupObject,
    outer_key: usize,
    inner_key: usize,
    probe_keys: Vec<usize>,
) -> (Vec<Vec<Datum>>, Vec<Vec<Datum>>) {
    let outer_width = outer_rows[0].len();
    let inner_width = table.visible_column_count();
    let types: Vec<FieldType> = std::iter::repeat_n(long(), outer_width + inner_width).collect();
    let conditions = vec![equality(outer_key, inner_key, outer_width)];
    let meta = || ExecutorMeta::new(schema_of(outer_width + inner_width), 0, INIT_CAP, CHUNK);
    let memory = crate::StmtContext::for_query().statement_memory();

    let mut hashed = JoinExec::new(
        meta(),
        kind,
        conditions.clone(),
        outer_source(outer_rows),
        scan_of(table, inner_width),
        crate::StmtContext::for_query(),
        memory.clone(),
    );
    let hash_rows = drain(&mut hashed, &types);

    let mut looked_up = JoinExec::new(
        meta(),
        kind,
        conditions,
        outer_source(outer_rows),
        scan_of(table, inner_width),
        crate::StmtContext::for_query(),
        memory,
    );
    looked_up.set_index_lookup_plan(IndexLookupPlan {
        lookup_is_left: false,
        probe_keys,
        source: crate::join::IndexLookupSource::Leaf(lookup_source(table, object, inner_width)),
        aggregation: None,
        aggregation_stream_ordered: false,
        outer_not_null: Vec::new(),
        inner_not_null: Vec::new(),
        probe_cast: None,
            probe_bounds: Vec::new(),
    });
    assert!(looked_up.is_index_join());
    let index_rows = drain(&mut looked_up, &types);
    (hash_rows, index_rows)
}

/// `n` outer rows whose key cycles over `keys`, so the same key recurs in
/// several DIFFERENT batches -- the shape a per-batch lookup that leaked
/// state between batches would get wrong.
fn cycling_outer(n: i64, keys: &[i64]) -> Vec<Vec<Datum>> {
    (0..n)
        .map(|i| vec![Datum::Int(i), Datum::Int(keys[(i as usize) % keys.len()])])
        .collect()
}

/// The index probe reads exactly the rows the hash join matches -- including
/// the duplicate-key case, where one outer row must emit SEVERAL inner rows.
///
/// MUTATION PROBE: build the probe from the wrong outer column
/// (`row[outer_offset(..)]` -> `row[0]` in `load_index_batch`) and the lookup
/// reads ranges for the row ids instead of the keys. Also caught: a lookup
/// map that keeps ONE inner row per key, and a probe range whose high bound
/// is excluded.
#[test]
fn the_index_lookup_and_the_hash_join_agree_row_for_row() {
    // `b` repeats: key 2 carries three inner rows, key 5 carries none.
    let table = inner_table(&[(10, 1), (11, 2), (12, 2), (13, 2), (14, 3), (15, 4)]);
    let outer = cycling_outer(40, &[1, 2, 3, 4, 5]);
    let (hash_rows, index_rows) = both_ways(
        JoinKind::Inner,
        &outer,
        &table,
        LookupObject::Index(1),
        1,
        1,
        vec![0],
    );
    assert!(!hash_rows.is_empty(), "the fixture must produce matches");
    assert_eq!(index_rows, hash_rows);
}

/// A LEFT join's unmatched outer rows survive, padded, and in outer order.
///
/// This is the test a lookup that dropped the miss -- or emitted it in the
/// wrong place -- fails: key 5 has no inner row at all, and it appears every
/// fifth outer row.
#[test]
fn a_left_join_pads_the_outer_rows_the_lookup_found_nothing_for() {
    let table = inner_table(&[(10, 1), (11, 2), (12, 2), (14, 3), (15, 4)]);
    let outer = cycling_outer(40, &[1, 2, 3, 4, 5]);
    let (hash_rows, index_rows) = both_ways(
        JoinKind::Left,
        &outer,
        &table,
        LookupObject::Index(1),
        1,
        1,
        vec![0],
    );
    assert_eq!(index_rows, hash_rows);
    assert_eq!(
        index_rows
            .iter()
            .filter(|row| row[3] == Datum::Null)
            .count(),
        8,
        "the eight outer rows carrying key 5 are padded, not dropped"
    );
}

/// A NULL outer key matches nothing and is never probed -- Go's
/// `constructDatumLookupKey` returning nil.
#[test]
fn a_null_outer_key_matches_nothing() {
    let table = inner_table(&[(10, 1), (11, 2)]);
    let outer = vec![
        vec![Datum::Int(0), Datum::Int(1)],
        vec![Datum::Int(1), Datum::Null],
        vec![Datum::Int(2), Datum::Int(2)],
    ];
    let (hash_rows, index_rows) = both_ways(
        JoinKind::Left,
        &outer,
        &table,
        LookupObject::Index(1),
        1,
        1,
        vec![0],
    );
    assert_eq!(index_rows, hash_rows);
    assert_eq!(index_rows.len(), 3);
    assert_eq!(index_rows[1][3], Datum::Null);
}

/// The clustered-handle object: one probe reads exactly one row, and the
/// answer is still the hash join's.
#[test]
fn a_handle_probe_reads_the_row_the_key_names() {
    let mut table = KvTable::new(92, vec![column("a", 1), column("b", 2)]);
    table.set_pk_handle_offset(0);
    for i in 1..=20i64 {
        table
            .insert_row(&[Datum::Int(i), Datum::Int(i * 10)], &tidb_expr::NoColumns)
            .unwrap();
    }
    // Keys 21..25 name no row at all.
    let outer: Vec<Vec<Datum>> = (1..=25i64)
        .map(|i| vec![Datum::Int(i), Datum::Int(i)])
        .collect();
    let (hash_rows, index_rows) = both_ways(
        JoinKind::Left,
        &outer,
        &table,
        LookupObject::Handle,
        1,
        0,
        vec![0],
    );
    assert_eq!(index_rows, hash_rows);
    assert_eq!(index_rows.len(), 25);
}

/// A complete common-handle tuple is a one-row record-key range.
#[test]
fn a_complete_common_handle_probe_reads_one_record() {
    let table = common_handle_table(&[(1, 1), (1, 2), (2, 1)]);
    let mut source = lookup_source(&table, LookupObject::CommonHandle, 2);
    source.set_probe_parts(vec![
        LookupProbePart::Dynamic(0),
        LookupProbePart::Dynamic(1),
    ]);
    source.set_probes(crate::access_path::IndexJoinProbes {
        keys: vec![vec![Datum::Int(1), Datum::Int(2)]],
        bound_values: Vec::new(),
    });

    assert_eq!(
        drain(&mut source, &[long(), long()]),
        vec![vec![Datum::Int(1), Datum::Int(2)]]
    );
}

/// Go's `buildDataSource2TableScanByIndexJoinProp` accepts a leading prefix of
/// a clustered common handle. The probe is a record-key range and can return
/// several rows; it must not be lowered to a secondary `PRIMARY` index lookup.
#[test]
fn a_common_handle_prefix_probe_reads_every_matching_record() {
    let table = common_handle_table(&[(1, 1), (1, 2), (1, 3), (2, 1)]);
    let mut source = lookup_source(&table, LookupObject::CommonHandle, 2);
    source.set_probe_parts(vec![LookupProbePart::Dynamic(0)]);
    source.set_probes(crate::access_path::IndexJoinProbes {
        keys: vec![vec![Datum::Int(1)]],
        bound_values: Vec::new(),
    });

    assert_eq!(
        drain(&mut source, &[long(), long()]),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(1), Datum::Int(3)],
        ]
    );
}

/// Go's DDL does not expose a clustered PRIMARY as a secondary index, but its
/// table path still builds common-handle record ranges from leading probes.
#[test]
fn a_ddl_common_handle_prefix_probe_reads_every_matching_record() {
    let table = ddl_common_handle_table(&[(1, 1), (1, 2), (1, 3), (2, 1)]);
    let mut source = lookup_source(&table, LookupObject::CommonHandle, 2);
    source.set_probe_parts(vec![LookupProbePart::Dynamic(0)]);
    source.set_probes(crate::access_path::IndexJoinProbes {
        keys: vec![vec![Datum::Int(1)]],
        bound_values: Vec::new(),
    });

    assert_eq!(
        drain(&mut source, &[long(), long()]),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(1), Datum::Int(3)],
        ]
    );
}

/// The batch boundary is a PERFORMANCE decision, not an answer: the same
/// join over 1 outer row, over exactly one batch's worth, and over several
/// batches produces the same rows in the same order.
///
/// MUTATION PROBE: drop the `state.cursor = 0` that rewinds the batch in
/// `load_index_batch` and every case here EXCEPT the single-batch ones fails.
///
/// A SURVIVOR, reported rather than hidden: changing the fill loop's bound
/// from `state.outer.len() < state.batch_size` to `<=` -- one row too many
/// per batch -- changes nothing, and cannot. The probes are built from the
/// batch AFTER it is filled, so a batch of the wrong SIZE is still a batch
/// whose probes and lookup map cover exactly its own rows. That is the
/// property this test really states: the boundary is free to move, the
/// rewind is not.
#[test]
fn the_batch_boundary_does_not_change_the_result() {
    let table = inner_table(&[(10, 1), (11, 2), (12, 2), (13, 3), (14, 4), (15, 7)]);
    for n in [1i64, 2, CHUNK as i64, CHUNK as i64 + 1, 3000] {
        let outer = cycling_outer(n, &[1, 2, 3, 4, 5, 6, 7]);
        let (hash_rows, index_rows) = both_ways(
            JoinKind::Left,
            &outer,
            &table,
            LookupObject::Index(1),
            1,
            1,
            vec![0],
        );
        assert_eq!(
            index_rows,
            hash_rows,
            "the {n}-row outer side crosses {} batch boundaries",
            n / CHUNK as i64
        );
    }
}

/// The lookup DEDUPES its probes: an outer batch whose rows all carry the
/// same key reads the inner side once, not once per row -- and still emits
/// one output row per outer row.
///
/// MUTATION PROBE: drop the `seen.insert(encoded)` guard in
/// `load_index_batch` and `produced_rows` multiplies by the batch size, which
/// is what the second assertion catches -- the row count alone would not,
/// because the lookup map answers per outer row either way. A dedup that
/// merged DIFFERENT keys instead (`seen.insert(encoded.len())`) fails five of
/// the six tests here on the rows themselves.
#[test]
fn one_key_repeated_across_a_batch_is_probed_once() {
    let table = inner_table(&[(10, 1), (11, 1), (12, 2)]);
    let outer: Vec<Vec<Datum>> = (0..100i64)
        .map(|i| vec![Datum::Int(i), Datum::Int(1)])
        .collect();
    let inner_width = 2;
    let types: Vec<FieldType> = std::iter::repeat_n(long(), 4).collect();
    let memory = crate::StmtContext::for_query().statement_memory();
    let source = lookup_source(&table, LookupObject::Index(1), inner_width);
    let produced = source.produced_rows();
    let mut exec = JoinExec::new(
        ExecutorMeta::new(schema_of(4), 0, INIT_CAP, CHUNK),
        JoinKind::Inner,
        vec![equality(1, 1, 2)],
        outer_source(&outer),
        scan_of(&table, inner_width),
        crate::StmtContext::for_query(),
        memory,
    );
    exec.set_index_lookup_plan(IndexLookupPlan {
        lookup_is_left: false,
        probe_keys: vec![0],
        source: crate::join::IndexLookupSource::Leaf(source),
        aggregation: None,
        aggregation_stream_ordered: false,
        outer_not_null: Vec::new(),
        inner_not_null: Vec::new(),
        probe_cast: None,
            probe_bounds: Vec::new(),
    });
    let rows = drain(&mut exec, &types);
    assert_eq!(
        rows.len(),
        200,
        "each of 100 outer rows matches both inners"
    );
    assert_eq!(
        produced.get(),
        2,
        "the whole batch shares one key, so the inner side is read once"
    );
}

/// The decision itself: WHICH side, WHICH object, WHICH range text.
///
/// `index_join_decision` is what `driver::from::build_join` calls, and it is
/// reached only where [`crate::driver::join_search`] answered `Index` -- which
/// over the whole replay is nowhere, so everything below is pinned by these
/// tests alone. See `crate::tests_join_search` for the census that says so.
mod decision {
    use super::{column, common_handle_table, inner_table, long};
    use crate::access_path::LookupObject;
    use crate::driver::index_join_decision::{index_join_decision, JoinSide};
    use crate::hash_join::EquiKey;
    use crate::join::JoinKind;
    use crate::kv_table::KvTable;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    fn key() -> Vec<EquiKey> {
        vec![EquiKey {
            left: 1,
            right: 1,
            class: crate::hash_join::KeyClass::Int,
            null_safe: false,
        }]
    }

    fn outer_side<'a>() -> JoinSide<'a> {
        JoinSide {
            table: None,
            visible: String::new(),
            types: vec![long(), long()],
            // The projected expression Go prints as a bare `Column`.
            names: vec!["Column".to_owned(), "Column".to_owned()],
            origin: None,
            source_visible: String::new(),
            output_to_source: vec![None, None],
            source_filters: Vec::new(),
            aggregation: None,
            aggregation_info: None,
            aggregation_final_info: None,
            aggregation_partial_info: None,
            composite: false,
        }
    }

    /// The inner side as `build_join` builds it: the types are the CHILD
    /// EXECUTOR's, which for a base table read whole are the table's own.
    fn inner_side(table: &KvTable) -> JoinSide<'_> {
        aliased_inner_side(table, "t")
    }

    /// The same side written under `visible`, whose `origin` stays the
    /// table's own `db.t` however it is aliased -- which is what the range
    /// text is qualified from.
    fn aliased_inner_side<'a>(table: &'a KvTable, visible: &str) -> JoinSide<'a> {
        JoinSide {
            table: Some(table),
            visible: visible.to_owned(),
            types: table
                .visible_columns()
                .iter()
                .map(|column| column.field_type.clone())
                .collect(),
            names: vec![format!("db.{visible}.a"), format!("db.{visible}.b")],
            origin: Some("db.t".to_owned()),
            source_visible: visible.to_owned(),
            output_to_source: vec![Some(0), Some(1)],
            source_filters: Vec::new(),
            aggregation: None,
            aggregation_info: None,
            aggregation_final_info: None,
            aggregation_partial_info: None,
            composite: false,
        }
    }

    /// An index on the join key, with Go's
    /// `eq(<index column>, <outer key>)` range text.
    #[test]
    fn an_index_on_the_join_key_is_a_candidate_with_gos_range_text() {
        let table = inner_table(&[(1, 1)]);
        let decision = index_join_decision(
            JoinKind::Inner,
            &key(),
            &outer_side(),
            &inner_side(&table),
            false,
        )
        .expect("an index on `b` and a computed outer key");
        assert!(!decision.lookup_is_left);
        assert_eq!(decision.probe_keys, vec![0]);
        assert_eq!(decision.range_info, "[eq(db.t.b, Column)]");
    }

    /// A join key that covers only the leading common-handle column still
    /// builds Go's table range path. It is not unique until every primary-key
    /// component is fixed.
    #[test]
    fn a_common_handle_prefix_is_a_non_unique_table_path() {
        let table = common_handle_table(&[(1, 1), (1, 2), (2, 1)]);
        let keys = vec![EquiKey {
            left: 1,
            right: 0,
            class: crate::hash_join::KeyClass::Int,
            null_safe: false,
        }];
        let decision = index_join_decision(
            JoinKind::Inner,
            &keys,
            &outer_side(),
            &inner_side(&table),
            false,
        )
        .expect("the leading clustered-key column builds a table range");

        assert!(matches!(decision.object, LookupObject::CommonHandle));
        assert_eq!(decision.probe_keys, vec![0]);
        assert_eq!(decision.probe_parts.len(), 1);
        assert!(!decision.max_one_row());
    }

    /// An ALIAS renames the access object and NOTHING inside the range.
    ///
    /// Go prints the looked-up column through `Column.StringWithCtx`, which
    /// reads `OrigName` -- the name the column was DECLARED under, which no
    /// alias touches. The recorded plan states both halves in one row:
    /// `r/planner/core/join_reorder_through_projection.result:1056` reads
    /// `table:outer_t, index:b(b)` and, on the same line,
    /// `range: decided by [eq(planner__core__join_reorder_through_projection
    /// .t1.b, Column)]`. Qualifying the range off the scope printed the alias
    /// in both places, which is a plan text TiDB never writes.
    #[test]
    fn an_aliased_inner_side_keeps_the_tables_own_name_inside_the_range() {
        let table = inner_table(&[(1, 1)]);
        let decision = index_join_decision(
            JoinKind::Inner,
            &key(),
            &outer_side(),
            &aliased_inner_side(&table, "outer_t"),
            false,
        )
        .expect("an index on `b` and a computed outer key");
        assert_eq!(
            decision.range_info, "[eq(db.t.b, Column)]",
            "the range names the table, not the alias it is written under"
        );
        assert_eq!(
            decision.visible, "outer_t",
            "while the access object DOES name the alias"
        );
    }

    /// A merge join already chosen wins: Go never costs an index join for a
    /// property the merge path already satisfies here.
    #[test]
    fn a_chosen_merge_join_refuses_the_candidate() {
        let table = inner_table(&[(1, 1)]);
        assert!(index_join_decision(
            JoinKind::Inner,
            &key(),
            &outer_side(),
            &inner_side(&table),
            true,
        )
        .is_none());
    }

    /// A LEFT join may not look up its PRESERVED side.
    #[test]
    fn an_outer_join_never_looks_up_the_preserved_side() {
        let table = inner_table(&[(1, 1)]);
        // The table is on the LEFT, which a LEFT join preserves.
        assert!(index_join_decision(
            JoinKind::Left,
            &key(),
            &inner_side(&table),
            &outer_side(),
            false,
        )
        .is_none());
    }

    /// WHAT THE OUTER KEY'S NAME NO LONGER DECIDES.
    ///
    /// This decision used to refuse a NAMED outer key -- a proxy for "no
    /// index can pre-sort this key, so Go is choosing between an index join
    /// and a hash join". The proxy is gone: whether the index strategy is
    /// admissible at all is `driver::join_search`'s question, asked of Go's
    /// own enumeration under the property the site was required to produce.
    /// This decision answers only WHICH side, WHICH object and WHICH ranges,
    /// and the name is now irrelevant to it.
    #[test]
    fn a_named_outer_key_is_no_longer_this_decisions_business() {
        let table = inner_table(&[(1, 1)]);
        let mut outer = outer_side();
        outer.names = vec!["db.o.a".to_owned(), "db.o.b".to_owned()];
        let decision =
            index_join_decision(JoinKind::Inner, &key(), &outer, &inner_side(&table), false)
                .expect("the side is still a base table with an index on the key");
        assert!(
            !decision.lookup_is_left,
            "the inner side is the RIGHT one here"
        );
        assert_eq!(decision.range_info, "[eq(db.t.b, db.o.b)]");
    }

    /// An unsigned indexed column is refused against a signed probe: the
    /// index entries were written under a different encoding, and Go's
    /// per-value `ConvertTo` is not ported.
    #[test]
    fn a_signed_probe_of_an_unsigned_index_column_is_refused() {
        let mut b = column("b", 2);
        b.field_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
        let mut table = KvTable::new(93, vec![column("a", 1), b]);
        table
            .create_index_with_context(
                crate::kv_table::KvIndex {
                    id: 1,
                    name: "ib".to_owned(),
                    comment: String::new(),
                    unique: false,
                    column_offsets: vec![1],
                    prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
                    visible: true,
                    global: false,
                    clustered_primary: false,
                },
                &crate::StmtContext::for_query(),
            )
            .unwrap();
        table
            .insert_row(&[Datum::Int(1), Datum::UInt(1)], &tidb_expr::NoColumns)
            .unwrap();
        assert!(index_join_decision(
            JoinKind::Inner,
            &key(),
            &outer_side(),
            &inner_side(&table),
            false,
        )
        .is_none());
    }
}

/// Go's `rule_join_key_type_cast` probe: the outer key is
/// `cast(str AS SIGNED)` computed behind the rule's guard
/// (`driver::join_key_cast`), probing the inner table's integer handle.
///
/// The oracle is this unit's own nested loop over the SAME data -- the
/// original double equality `eq(varchar, int)` evaluated per pair -- which
/// the replay corpus already proves against TiDB. The guard is what makes
/// the two agree: without it, `'1.5'` casts to 2 and the probe would read
/// handle 2, a row the double equality rejects. `'abc'` casts to 0 AND its
/// double value is 0.0, so it must match the id-0 row; a NULL key matches
/// nothing.
#[test]
fn a_cast_probe_guards_and_matches_like_the_double_equality() {
    let mut table = KvTable::new(96, vec![column("id", 1), column("v", 2)]);
    table.set_pk_handle_offset(0);
    for (id, v) in [(0i64, 0i64), (1, 10), (2, 20), (3, 30)] {
        table
            .insert_row(&[Datum::Int(id), Datum::Int(v)], &tidb_expr::NoColumns)
            .unwrap();
    }
    let varchar = {
        let mut ft = FieldType::new(FieldTypeCode::Varchar);
        ft.set_flen(20);
        ft.set_charset_name("utf8mb4");
        ft.set_collation_name("utf8mb4_bin");
        ft
    };
    // outer(rowid bigint, key varchar(20)).
    let outer_column = |index: usize, ft: FieldType| {
        let mut c = Column::new(index as i64 + 1, ft);
        c.index = index as i64;
        c
    };
    let outer_schema = Schema::new(vec![
        outer_column(0, long()),
        outer_column(1, varchar.clone()),
    ]);
    let outer_rows: Vec<Vec<Datum>> = [
        Some("1"),
        Some("2"),
        Some("1.5"),
        Some("abc"),
        None,
        Some("2"),
    ]
    .iter()
    .enumerate()
    .map(|(i, key)| {
        vec![
            Datum::Int(i as i64),
            key.map_or(Datum::Null, |k| Datum::Bytes(k.as_bytes().to_vec())),
        ]
    })
    .collect();
    let joined_schema = || {
        Schema::new(vec![
            outer_column(0, long()),
            outer_column(1, varchar.clone()),
            outer_column(2, long()),
            outer_column(3, long()),
        ])
    };
    let types = [long(), varchar.clone(), long(), long()];
    // The ORIGINAL equality, `eq(varchar, int)` over the joined row: no
    // split key comes of it, so the reference join is the nested loop
    // evaluating the double comparison -- and the index side retains it as
    // its residual check.
    let condition = {
        let column = |index: usize, ft: FieldType| {
            let mut c = Column::new(index as i64 + 1, ft);
            c.index = index as i64;
            Expression::Column(c)
        };
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            long(),
            vec![column(1, varchar.clone()), column(2, long())],
        ))
    };
    let outer_exec = || {
        Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(outer_schema.clone(), 0, INIT_CAP, CHUNK),
            outer_rows.clone(),
        )) as Box<dyn Executor>
    };
    let meta = || ExecutorMeta::new(joined_schema(), 0, INIT_CAP, CHUNK);
    let memory = crate::StmtContext::for_query().statement_memory();

    let mut nested = JoinExec::new(
        meta(),
        JoinKind::Inner,
        vec![condition.clone()],
        outer_exec(),
        scan_of(&table, 2),
        crate::StmtContext::for_query(),
        memory.clone(),
    );
    let nested_rows = drain(&mut nested, &types);

    let ctx = crate::StmtContext::for_query();
    let rewrite = crate::driver::join_key_cast::build_rewrite(&long(), &varchar, &ctx)
        .expect("the cast and guard build over one string column");
    let mut looked_up = JoinExec::new(
        meta(),
        JoinKind::Inner,
        vec![condition],
        outer_exec(),
        scan_of(&table, 2),
        crate::StmtContext::for_query(),
        memory,
    );
    looked_up.set_index_lookup_plan(IndexLookupPlan {
        lookup_is_left: false,
        probe_keys: Vec::new(),
        source: crate::join::IndexLookupSource::Leaf(lookup_source(
            &table,
            LookupObject::Handle,
            2,
        )),
        aggregation: None,
        aggregation_stream_ordered: false,
        outer_not_null: Vec::new(),
        inner_not_null: Vec::new(),
        probe_cast: Some(crate::join::IndexProbeCast {
            outer_offset: 1,
            inner_offset: 0,
            cast: rewrite.cast,
            guard: rewrite.guard,
            str_type: rewrite.str_type,
        }),
        probe_bounds: Vec::new(),
    });
    assert!(looked_up.is_index_join());
    let index_rows = drain(&mut looked_up, &types);

    // The joined (outer rowid, inner id) pairs: '1' and '2' land on their
    // handles, 'abc' casts to 0 and its DOUBLE value is 0.0 so it matches
    // id 0, '1.5' and NULL match nothing.
    let id_pairs = |rows: &[Vec<Datum>]| {
        rows.iter()
            .map(|row| (row[0].clone(), row[2].clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        id_pairs(&nested_rows),
        vec![
            (Datum::Int(0), Datum::Int(1)),
            (Datum::Int(1), Datum::Int(2)),
            (Datum::Int(3), Datum::Int(0)),
            (Datum::Int(5), Datum::Int(2)),
        ],
        "the double-equality oracle moved",
    );
    assert_eq!(index_rows, nested_rows, "the cast probe disagrees with it");
}

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

//! Go's `mppExec` chain as `handleCopDAGRequest` runs it: the per-executor
//! execution summaries and the processed versions of the scan detail.
//!
//! Every expected figure was captured from Go TiDB on unistore with a
//! throwaway `pkg/testkit` probe running `EXPLAIN ANALYZE` over the same
//! data: each cop executor's `actRows` is its `NumProducedRows`, and
//! `scan_detail.total_process_keys` is `ScanDetailV2.ProcessedVersions`.
//! The probe's tables:
//!
//! ```sql
//! CREATE TABLE t (a INT PRIMARY KEY, b INT, c VARCHAR(20), KEY(b));
//! INSERT INTO t VALUES (1,1,'x'),(2,2,'y'),(3,3,'z'),(4,4,'w'),(5,5,'v'),(6,6,'u');
//! CREATE TABLE big (a INT PRIMARY KEY, b INT, KEY(b)); -- (i, i % 7) for i in 1..=200
//! ```

use super::*;
use tidb_datatype::Datum;
use tidb_proto::{KvrpcMutation, KvrpcOp};
use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
use tidb_unistore_tikv::mvcc_store::MvccStore;

const TABLE_ID: i64 = 90;
const INDEX_ID: i64 = 1;

fn put(store: &mut MvccStore, key: Vec<u8>, value: Vec<u8>) {
    store
        .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
            mutations: vec![KvrpcMutation {
                op: KvrpcOp::Put as i32,
                key: key.clone(),
                value,
                ..KvrpcMutation::default()
            }],
            primary_lock: key.clone(),
            start_version: 10,
            ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
        })
        .expect("prewrites");
    store.commit(&[key], 10, 11).expect("commits");
}

fn row_key(handle: i64) -> Vec<u8> {
    encode_row_key_with_handle(TABLE_ID, &RecordHandle::Int(handle))
}

/// The `KEY(b)` entry prefix for `values`.
fn index_key(values: &[Datum]) -> Vec<u8> {
    let mut key = tidb_tablecodec::table_key::gen_table_index_prefix(TABLE_ID);
    tidb_codec::encode_int(&mut key, INDEX_ID);
    key.extend(tidb_codec::encode_key(values).expect("an index key encodes"));
    key
}

/// Rows `(a, b, c)`: the record (old row format) and its `KEY(b)` entry.
fn seed(store: &mut MvccStore, rows: &[(i64, i64, Option<&str>)]) {
    for (a, b, c) in rows {
        let mut value = vec![Datum::Int(2), Datum::Int(*b)];
        if let Some(c) = c {
            value.extend([Datum::Int(3), Datum::Bytes(c.as_bytes().to_vec())]);
        }
        put(store, row_key(*a), tidb_codec::encode_value(&value).expect("row"));
        put(store, index_key(&[Datum::Int(*b), Datum::Int(*a)]), b"0".to_vec());
    }
}

/// Table `t` of the probe.
fn seed_t(store: &mut MvccStore) {
    seed(
        store,
        &[
            (1, 1, Some("x")),
            (2, 2, Some("y")),
            (3, 3, Some("z")),
            (4, 4, Some("w")),
            (5, 5, Some("v")),
            (6, 6, Some("u")),
        ],
    );
}

/// Table `big` of the probe.
fn seed_big(store: &mut MvccStore) {
    let rows = (1..=200).map(|i| (i, i % 7, None)).collect::<Vec<_>>();
    seed(store, &rows);
}

fn column(id: i64, tp: i32, pk_handle: bool) -> tipb::ColumnInfo {
    tipb::ColumnInfo {
        column_id: Some(id),
        tp: Some(tp),
        pk_handle: Some(pk_handle),
        collation: (tp == 15).then(|| tidb_datatype::collation_to_proto("utf8mb4_bin")),
        ..tipb::ColumnInfo::default()
    }
}

/// `TableFullScan` over `a, b, c`.
fn table_scan(desc: bool) -> tipb::Executor {
    tipb::Executor {
        tp: Some(tipb::ExecType::TypeTableScan as i32),
        tbl_scan: Some(tipb::TableScan {
            table_id: Some(TABLE_ID),
            columns: vec![column(1, 8, true), column(2, 8, false), column(3, 15, false)],
            desc: Some(desc),
            ..tipb::TableScan::default()
        }),
        ..tipb::Executor::default()
    }
}

/// `IndexFullScan` over `KEY(b)`, reading `b, a`.
fn index_scan() -> tipb::Executor {
    tipb::Executor {
        tp: Some(tipb::ExecType::TypeIndexScan as i32),
        idx_scan: Some(tipb::IndexScan {
            table_id: Some(TABLE_ID),
            index_id: Some(INDEX_ID),
            columns: vec![column(2, 8, false), column(1, 8, true)],
            ..tipb::IndexScan::default()
        }),
        ..tipb::Executor::default()
    }
}

fn limit(count: u64) -> tipb::Executor {
    tipb::Executor {
        tp: Some(tipb::ExecType::TypeLimit as i32),
        limit: Some(Box::new(tipb::Limit {
            limit: Some(count),
            ..tipb::Limit::default()
        })),
        ..tipb::Executor::default()
    }
}

fn varchar_type() -> tipb::FieldType {
    tipb::FieldType {
        tp: Some(15),
        charset: Some("utf8mb4".to_owned()),
        collate: Some(tidb_datatype::collation_to_proto("utf8mb4_bin")),
        ..tipb::FieldType::default()
    }
}

fn column_ref(offset: i64, field_type: tipb::FieldType) -> tipb::Expr {
    let mut val = Vec::new();
    tidb_codec::encode_int(&mut val, offset);
    tipb::Expr {
        tp: Some(tipb::ExprType::ColumnRef as i32),
        val: Some(val),
        field_type: Some(field_type),
        ..tipb::Expr::default()
    }
}

fn longlong_type() -> tipb::FieldType {
    tipb::FieldType {
        tp: Some(8),
        ..tipb::FieldType::default()
    }
}

/// `Selection(c > literal)` over the table scan's `c`.
fn c_greater_than(literal: &str) -> tipb::Executor {
    tipb::Executor {
        tp: Some(tipb::ExecType::TypeSelection as i32),
        selection: Some(Box::new(tipb::Selection {
            conditions: vec![tipb::Expr {
                tp: Some(tipb::ExprType::ScalarFunc as i32),
                sig: Some(tipb::ScalarFuncSig::GtString as i32),
                children: vec![
                    column_ref(2, varchar_type()),
                    tipb::Expr {
                        tp: Some(tipb::ExprType::String as i32),
                        val: Some(literal.as_bytes().to_vec()),
                        field_type: Some(varchar_type()),
                        ..tipb::Expr::default()
                    },
                ],
                field_type: Some(longlong_type()),
                ..tipb::Expr::default()
            }],
            ..tipb::Selection::default()
        })),
        ..tipb::Executor::default()
    }
}

/// `TopN(ORDER BY c LIMIT count)` over the table scan's `c`.
fn top_n_by_c(count: u64) -> tipb::Executor {
    tipb::Executor {
        tp: Some(tipb::ExecType::TypeTopN as i32),
        top_n: Some(Box::new(tipb::TopN {
            order_by: vec![tipb::ByItem {
                expr: Some(column_ref(2, varchar_type())),
                desc: Some(false),
                ..tipb::ByItem::default()
            }],
            limit: Some(count),
            ..tipb::TopN::default()
        })),
        ..tipb::Executor::default()
    }
}

/// `StreamAgg(count(*) [GROUP BY b])` over the index scan.
fn count_star(group_by_b: bool) -> tipb::Executor {
    let mut one = Vec::new();
    tidb_codec::encode_int(&mut one, 1);
    tipb::Executor {
        tp: Some(tipb::ExecType::TypeStreamAgg as i32),
        aggregation: Some(Box::new(tipb::Aggregation {
            agg_func: vec![tipb::Expr {
                tp: Some(tipb::ExprType::Count as i32),
                children: vec![tipb::Expr {
                    tp: Some(tipb::ExprType::Int64 as i32),
                    val: Some(one),
                    field_type: Some(longlong_type()),
                    ..tipb::Expr::default()
                }],
                field_type: Some(longlong_type()),
                ..tipb::Expr::default()
            }],
            group_by: if group_by_b {
                vec![column_ref(0, longlong_type())]
            } else {
                Vec::new()
            },
            ..tipb::Aggregation::default()
        })),
        ..tipb::Executor::default()
    }
}

fn table_range() -> coprocessor::KeyRange {
    let (start, end) = tidb_tablecodec::table_key::get_table_handle_key_range(TABLE_ID);
    coprocessor::KeyRange { start, end }
}

fn index_range() -> coprocessor::KeyRange {
    let (start, end) = tidb_tablecodec::get_table_index_key_range(TABLE_ID, INDEX_ID);
    coprocessor::KeyRange { start, end }
}

/// What the response reports: each executor's produced rows, the processed
/// versions, and the rows' first column.
struct Answer {
    produced: Vec<u64>,
    processed_versions: u64,
    first_column: Vec<Datum>,
}

fn run(
    store: &mut MvccStore,
    executors: Vec<tipb::Executor>,
    ranges: Vec<coprocessor::KeyRange>,
) -> Answer {
    let width = match executors.last().map(tipb::Executor::tp) {
        Some(tipb::ExecType::TypeStreamAgg) => executors
            .last()
            .and_then(|agg| agg.aggregation.as_ref())
            .map_or(0, |agg| agg.agg_func.len() + agg.group_by.len()),
        _ => match executors[0].tp() {
            tipb::ExecType::TypeIndexScan => 2,
            _ => 3,
        },
    };
    let dag = tipb::DagRequest {
        executors,
        collect_execution_summaries: Some(true),
        ..tipb::DagRequest::default()
    };
    let resp = handle_cop_request(
        store,
        &coprocessor::Request {
            tp: REQ_TYPE_DAG,
            data: dag.encode_to_vec(),
            ranges,
            start_ts: 20,
            ..coprocessor::Request::default()
        },
    );
    assert!(resp.other_error.is_empty(), "{}", resp.other_error);
    let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("a select response");
    assert_eq!(select.error, None);
    let mut datums = Vec::new();
    for chunk in &select.chunks {
        let mut rest = chunk.rows_data.as_deref().unwrap_or_default();
        while !rest.is_empty() {
            let (remaining, datum) = tidb_codec::decode_one(rest).expect("a datum decodes");
            datums.push(datum);
            rest = remaining;
        }
    }
    let first_column = datums.chunks(width).map(|row| row[0].clone()).collect();
    Answer {
        produced: select
            .execution_summaries
            .iter()
            .map(|summary| summary.num_produced_rows())
            .collect(),
        processed_versions: resp
            .exec_details_v2
            .and_then(|details| details.scan_detail_v2)
            .map_or(0, |scan| scan.processed_versions),
        first_column,
    }
}

fn ints(values: &[i64]) -> Vec<Datum> {
    values.iter().copied().map(Datum::Int).collect()
}

/// `select b from t use index(b) limit 3`: `IndexFullScan` 6, `Limit` 3,
/// total_process_keys 6 -- the index scan reads every key in `open` and hands
/// its whole chunk up before `limitExec` truncates it.
#[test]
fn an_index_scan_under_a_limit_reads_its_whole_chunk() {
    let mut store = MvccStore::new();
    seed_t(&mut store);
    let answer = run(&mut store, vec![index_scan(), limit(3)], vec![index_range()]);
    assert_eq!(answer.produced, [6, 3]);
    assert_eq!(answer.processed_versions, 6);
    assert_eq!(answer.first_column, ints(&[1, 2, 3]));
}

/// `select * from t limit 3`: `TableFullScan` 6, `Limit` 3, total_process_keys 6.
#[test]
fn a_table_scan_under_a_limit_hands_up_its_whole_chunk() {
    let mut store = MvccStore::new();
    seed_t(&mut store);
    let answer = run(&mut store, vec![table_scan(false), limit(3)], vec![table_range()]);
    assert_eq!(answer.produced, [6, 3]);
    assert_eq!(answer.processed_versions, 6);
    assert_eq!(answer.first_column, ints(&[1, 2, 3]));
}

/// `select * from t order by c limit 2`: `TableFullScan` 6, `TopN` 6 (the
/// rows it consumed), total_process_keys 6; the rows are 'u' then 'v'.
#[test]
fn a_top_n_counts_the_rows_it_consumes() {
    let mut store = MvccStore::new();
    seed_t(&mut store);
    let answer = run(&mut store, vec![table_scan(false), top_n_by_c(2)], vec![table_range()]);
    assert_eq!(answer.produced, [6, 6]);
    assert_eq!(answer.processed_versions, 6);
    assert_eq!(answer.first_column, ints(&[6, 5]));
}

/// `select * from t where c > 'v'`: `TableFullScan` 6, `Selection` 4.
#[test]
fn a_selection_counts_the_rows_it_selects() {
    let mut store = MvccStore::new();
    seed_t(&mut store);
    let answer = run(&mut store, vec![table_scan(false), c_greater_than("v")], vec![table_range()]);
    assert_eq!(answer.produced, [6, 4]);
    assert_eq!(answer.processed_versions, 6);
    assert_eq!(answer.first_column, ints(&[1, 2, 3, 4]));
}

/// `select * from t where c > 'a' limit 2`: `TableFullScan` 6, `Selection` 6,
/// `Limit` 2.
#[test]
fn a_selection_under_a_limit_filters_the_whole_chunk() {
    let mut store = MvccStore::new();
    seed_t(&mut store);
    let answer = run(
        &mut store,
        vec![table_scan(false), c_greater_than("a"), limit(2)],
        vec![table_range()],
    );
    assert_eq!(answer.produced, [6, 6, 2]);
    assert_eq!(answer.processed_versions, 6);
    assert_eq!(answer.first_column, ints(&[1, 2]));
}

/// `select * from t where c > 'v' order by c limit 1`: `TableFullScan` 6,
/// `Selection` 4, `TopN` 4.
#[test]
fn a_top_n_over_a_selection_counts_the_selected_rows() {
    let mut store = MvccStore::new();
    seed_t(&mut store);
    let answer = run(
        &mut store,
        vec![table_scan(false), c_greater_than("v"), top_n_by_c(1)],
        vec![table_range()],
    );
    assert_eq!(answer.produced, [6, 4, 4]);
    assert_eq!(answer.processed_versions, 6);
    assert_eq!(answer.first_column, ints(&[4]));
}

/// `select count(*) from t`: `IndexFullScan` 6, `StreamAgg` 1.
#[test]
fn an_aggregation_counts_its_groups() {
    let mut store = MvccStore::new();
    seed_t(&mut store);
    let answer = run(&mut store, vec![index_scan(), count_star(false)], vec![index_range()]);
    assert_eq!(answer.produced, [6, 1]);
    assert_eq!(answer.processed_versions, 6);
    assert_eq!(answer.first_column, ints(&[6]));
}

/// `select b, count(*) from big group by b`: `IndexFullScan` 200,
/// `StreamAgg` 7, total_process_keys 200.
#[test]
fn a_grouped_aggregation_over_many_chunks_counts_its_groups() {
    let mut store = MvccStore::new();
    seed_big(&mut store);
    let answer = run(&mut store, vec![index_scan(), count_star(true)], vec![index_range()]);
    assert_eq!(answer.produced, [200, 7]);
    assert_eq!(answer.processed_versions, 200);
}

/// `select b from big use index(b) limit 3`: `IndexFullScan` 64 (the two
/// chunks `limitExec` pulled), `Limit` 3, total_process_keys 200 (`open`
/// scanned the whole range).
#[test]
fn an_index_scan_under_a_limit_scans_its_whole_range_in_open() {
    let mut store = MvccStore::new();
    seed_big(&mut store);
    let answer = run(&mut store, vec![index_scan(), limit(3)], vec![index_range()]);
    assert_eq!(answer.produced, [64, 3]);
    assert_eq!(answer.processed_versions, 200);
}

/// `select * from big where b = 3 limit 5`: `IndexRangeScan` 29, `Limit` 5,
/// total_process_keys 29.
#[test]
fn an_index_range_scan_under_a_limit_reads_the_range() {
    let mut store = MvccStore::new();
    seed_big(&mut store);
    let range = coprocessor::KeyRange {
        start: index_key(&[Datum::Int(3)]),
        end: index_key(&[Datum::Int(4)]),
    };
    let answer = run(&mut store, vec![index_scan(), limit(5)], vec![range]);
    assert_eq!(answer.produced, [29, 5]);
    assert_eq!(answer.processed_versions, 29);
}

/// `select * from big limit 3`: `TableFullScan` 64, `Limit` 3,
/// total_process_keys 96. The scan goroutine handed its first chunk to the
/// waiting `next`, buffered the second, and blocked on the third; `next`'s
/// second receive moved the third into the slot, and `stop` closed `done`
/// before the woken goroutine processed another key.
#[test]
fn a_table_scan_goroutine_runs_two_chunks_ahead_of_a_limit() {
    let mut store = MvccStore::new();
    seed_big(&mut store);
    let answer = run(&mut store, vec![table_scan(false), limit(3)], vec![table_range()]);
    assert_eq!(answer.produced, [64, 3]);
    assert_eq!(answer.processed_versions, 96);
}

/// `select * from big limit 40`: `TableFullScan` 96, `Limit` 40,
/// total_process_keys 96 -- the third chunk was already in the slot, so the
/// goroutine never ran again before `stop`.
#[test]
fn a_table_scan_under_a_larger_limit_stops_where_the_goroutine_blocked() {
    let mut store = MvccStore::new();
    seed_big(&mut store);
    let answer = run(&mut store, vec![table_scan(false), limit(40)], vec![table_range()]);
    assert_eq!(answer.produced, [96, 40]);
    assert_eq!(answer.processed_versions, 96);
}

/// `select * from big where a between 1 and 50 or a between 100 and 150
/// limit 3`: `TableRangeScan` 64, `Limit` 3, total_process_keys 96 -- the
/// goroutine's chunks span both ranges.
#[test]
fn a_table_range_scan_goroutine_chunks_span_its_ranges() {
    let mut store = MvccStore::new();
    seed_big(&mut store);
    let ranges = vec![
        coprocessor::KeyRange {
            start: row_key(1),
            end: row_key(51),
        },
        coprocessor::KeyRange {
            start: row_key(100),
            end: row_key(151),
        },
    ];
    let answer = run(&mut store, vec![table_scan(false), limit(3)], ranges);
    assert_eq!(answer.produced, [64, 3]);
    assert_eq!(answer.processed_versions, 96);
}

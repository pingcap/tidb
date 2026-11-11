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

//! Go `indexLookUpExec` (`mpp_exec.go`) and `buildIndexLookUp` (`mpp.go`)
//! as `handleCopDAGRequest` runs them: the table rows of the handles a local
//! region holds are the response's chunks, and the index rows of the others
//! are its intermediate output.

use std::cell::RefCell;

use super::*;
use tidb_datatype::{Datum, FieldType};
use tidb_proto::{errorpb, metapb, KvrpcMutation, KvrpcOp};
use tidb_tablecodec::table_key::{encode_row_key, encode_row_key_with_handle, RecordHandle};
use tidb_unistore_dbreader::{
    DbReader, ExtraDbReaderProvider, GetExtraDbReaderContext, KvPair, LocateExtraRegionResult, ScanReq,
};
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

fn mvcc_key(row_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::new();
    tidb_codec::encode_bytes(&mut key, row_key);
    key
}

fn index_prefix() -> Vec<u8> {
    let mut key = tidb_tablecodec::table_key::gen_table_index_prefix(TABLE_ID);
    tidb_codec::encode_int(&mut key, INDEX_ID);
    key
}

/// `CREATE TABLE t (a INT PRIMARY KEY, b INT, KEY(b))`: the rows `(a, a * 10)`
/// and their `KEY(b)` entries.
fn seed_int(store: &mut MvccStore, handles: &[i64]) {
    for a in handles {
        let value = tidb_codec::encode_value(&[Datum::Int(2), Datum::Int(a * 10)]).expect("row");
        put(store, row_key(*a), value);
        seed_int_index(store, *a, a * 10);
    }
}

fn seed_int_index(store: &mut MvccStore, a: i64, b: i64) {
    let mut key = index_prefix();
    key.extend(tidb_codec::encode_key(&[Datum::Int(b), Datum::Int(a)]).expect("index key"));
    put(store, key, b"0".to_vec());
}

fn common_handle(a: &str) -> Vec<u8> {
    tidb_codec::encode_key(&[Datum::Bytes(a.as_bytes().to_vec())]).expect("handle")
}

/// `CREATE TABLE t (a VARCHAR(20) PRIMARY KEY CLUSTERED, b INT, KEY(b))`:
/// the rows `(a, b)` and their `KEY(b)` entries.
fn seed_common(store: &mut MvccStore, rows: &[(&str, i64)]) {
    for (a, b) in rows {
        // A utf8mb4_bin VARCHAR key part is not its value (Go
        // `NeedRestoredData`), so the row stores the column too.
        let value = tidb_codec::encode_value(&[
            Datum::Int(1),
            Datum::Bytes(a.as_bytes().to_vec()),
            Datum::Int(2),
            Datum::Int(*b),
        ])
        .expect("row");
        put(store, encode_row_key(TABLE_ID, &common_handle(a)), value);
        let mut key = index_prefix();
        key.extend(tidb_codec::encode_key(&[Datum::Int(*b)]).expect("index key"));
        key.extend(common_handle(a));
        put(store, key, b"0".to_vec());
    }
}

/// A store whose key space is split into regions at `splits` (MVCC-encoded
/// keys); `local[i]` says whether this store leads region `i`, and
/// `unreadable[i]` whether reading it answers a region error.
struct RegionStore<'s> {
    store: &'s MvccStore,
    splits: Vec<Vec<u8>>,
    local: Vec<bool>,
    unreadable: Vec<bool>,
    /// The ranges of every `GetExtraDBReaderByRegion` call.
    read_ranges: RefCell<Vec<Vec<coprocessor::KeyRange>>>,
}

impl<'s> RegionStore<'s> {
    fn one_region(store: &'s MvccStore) -> Self {
        Self::split(store, Vec::new(), vec![true])
    }

    fn split(store: &'s MvccStore, splits: Vec<Vec<u8>>, local: Vec<bool>) -> Self {
        let unreadable = vec![false; local.len()];
        Self {
            store,
            splits,
            local,
            unreadable,
            read_ranges: RefCell::default(),
        }
    }

    fn region(&self, idx: usize) -> metapb::Region {
        metapb::Region {
            id: idx as u64 + 1,
            start_key: idx.checked_sub(1).map_or_else(Vec::new, |prev| self.splits[prev].clone()),
            end_key: self.splits.get(idx).cloned().unwrap_or_default(),
            ..metapb::Region::default()
        }
    }
}

impl DbReader for RegionStore<'_> {
    fn scan(&self, req: &ScanReq) -> Vec<KvPair> {
        self.store.scan(req)
    }

    fn extra_db_reader_provider(&self) -> Option<&dyn ExtraDbReaderProvider> {
        Some(self)
    }
}

impl ExtraDbReaderProvider for RegionStore<'_> {
    fn locate_extra_region(&self, key: &[u8]) -> Result<LocateExtraRegionResult, String> {
        let idx = self.splits.partition_point(|split| split.as_slice() <= key);
        if !self.local[idx] {
            return Ok(LocateExtraRegionResult::default());
        }
        Ok(LocateExtraRegionResult {
            found: true,
            region: Some(self.region(idx)),
            peer: Some(metapb::Peer::default()),
            is_leader: true,
        })
    }

    fn get_extra_db_reader_by_region<'s>(
        &'s self,
        ctx: &GetExtraDbReaderContext,
    ) -> Result<Box<dyn DbReader + 's>, errorpb::Error> {
        self.read_ranges.borrow_mut().push(ctx.ranges.clone());
        let idx = ctx.region.as_ref().expect("a located region").id as usize - 1;
        if self.unreadable[idx] {
            return Err(errorpb::Error {
                message: "epoch not match".to_owned(),
                ..errorpb::Error::default()
            });
        }
        Ok(Box::new(self.store))
    }
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

/// `IndexLookUp(IndexFullScan(b) -> b, a; TableRowIDScan -> a, b)`; a
/// clustered table's handle column is the VARCHAR primary key.
fn index_look_up(common: bool) -> tipb::Executor {
    let (a, primary_column_ids) = if common {
        (column(1, 15, false), vec![1])
    } else {
        (column(1, 8, true), Vec::new())
    };
    let b = column(2, 8, false);
    let index_scan = tipb::Executor {
        tp: Some(tipb::ExecType::TypeIndexScan as i32),
        idx_scan: Some(tipb::IndexScan {
            table_id: Some(TABLE_ID),
            index_id: Some(INDEX_ID),
            columns: vec![b.clone(), a.clone()],
            primary_column_ids: primary_column_ids.clone(),
            ..tipb::IndexScan::default()
        }),
        ..tipb::Executor::default()
    };
    let table_scan = tipb::Executor {
        tp: Some(tipb::ExecType::TypeTableScan as i32),
        tbl_scan: Some(tipb::TableScan {
            table_id: Some(TABLE_ID),
            columns: vec![a, b],
            primary_column_ids,
            ..tipb::TableScan::default()
        }),
        ..tipb::Executor::default()
    };
    tipb::Executor {
        tp: Some(tipb::ExecType::TypeIndexLookUp as i32),
        index_lookup: Some(tipb::IndexLookUp {
            index_handle_offsets: vec![1],
            children: vec![index_scan, table_scan],
            ..tipb::IndexLookUp::default()
        }),
        ..tipb::Executor::default()
    }
}

fn index_range() -> coprocessor::KeyRange {
    let (start, end) = tidb_tablecodec::get_table_index_key_range(TABLE_ID, INDEX_ID);
    coprocessor::KeyRange { start, end }
}

fn request(root: tipb::Executor) -> coprocessor::Request {
    let dag = tipb::DagRequest {
        root_executor: Some(root),
        // `flattenMppExec`: the index scan, the mock table child, the lookup.
        intermediate_output_channels: vec![tipb::IntermediateOutputChannel {
            executor_idx: 2,
            output_offsets: vec![0, 1],
        }],
        ..tipb::DagRequest::default()
    };
    coprocessor::Request {
        tp: REQ_TYPE_DAG,
        data: dag.encode_to_vec(),
        ranges: vec![index_range()],
        start_ts: 20,
        ..coprocessor::Request::default()
    }
}

fn decode_rows(chunks: &[tipb::Chunk]) -> Vec<Vec<Datum>> {
    let mut datums = Vec::new();
    for chunk in chunks {
        let mut rest = chunk.rows_data.as_deref().unwrap_or_default();
        while !rest.is_empty() {
            let (remaining, datum) = tidb_codec::decode_one(rest).expect("a datum decodes");
            datums.push(datum);
            rest = remaining;
        }
    }
    datums.chunks(2).map(<[Datum]>::to_vec).collect()
}

/// The table rows `(a, b)` the lookup answered and the index rows `(b, a)`
/// it left to the client.
fn run(store: &RegionStore<'_>, root: tipb::Executor) -> (Vec<Vec<Datum>>, Vec<Vec<Datum>>) {
    let resp = handle_cop_request(store, &request(root));
    assert!(resp.other_error.is_empty(), "{}", resp.other_error);
    let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("a select response");
    assert_eq!(select.error, None);
    assert_eq!(select.intermediate_outputs.len(), 1);
    let output = &select.intermediate_outputs[0];
    assert_eq!(output.encode_type(), tipb::EncodeType::TypeDefault);
    (decode_rows(&select.chunks), decode_rows(&output.chunks))
}

fn table_rows(handles: &[i64]) -> Vec<Vec<Datum>> {
    handles.iter().map(|a| vec![Datum::Int(*a), Datum::Int(a * 10)]).collect()
}

fn index_rows(handles: &[i64]) -> Vec<Vec<Datum>> {
    handles.iter().map(|a| vec![Datum::Int(a * 10), Datum::Int(*a)]).collect()
}

fn range(start: Vec<u8>, last: &[u8]) -> coprocessor::KeyRange {
    let mut end = last.to_vec();
    end.push(0);
    coprocessor::KeyRange { start, end }
}

/// Every handle is in the one local region: the response is the table rows,
/// read through one table scan whose ranges join consecutive int handles
/// (`h.Next().Compare(curHandles[i+1]) == 0`).
#[test]
fn local_rows_are_read_through_ranges_joining_consecutive_handles() {
    let mut store = MvccStore::new();
    seed_int(&mut store, &[1, 2, 4, 5, 6, 9]);
    let regions = RegionStore::one_region(&store);
    let (rows, left) = run(&regions, index_look_up(false));
    assert_eq!(rows, table_rows(&[1, 2, 4, 5, 6, 9]));
    assert_eq!(left, Vec::<Vec<Datum>>::new());
    assert_eq!(
        *regions.read_ranges.borrow(),
        [vec![
            range(row_key(1), &row_key(2)),
            range(row_key(4), &row_key(6)),
            range(row_key(9), &row_key(9)),
        ]]
    );
}

/// The handles of a region this store does not lead go back as index rows;
/// each local region is read through its own table scan.
#[test]
fn rows_of_a_region_elsewhere_are_left_to_the_client() {
    let mut store = MvccStore::new();
    seed_int(&mut store, &[1, 2, 3, 4, 5, 6]);
    let splits = vec![mvcc_key(&row_key(3)), mvcc_key(&row_key(5))];
    let regions = RegionStore::split(&store, splits, vec![true, false, true]);
    let (rows, left) = run(&regions, index_look_up(false));
    assert_eq!(rows, table_rows(&[1, 2, 5, 6]));
    assert_eq!(left, index_rows(&[3, 4]));
    assert_eq!(
        *regions.read_ranges.borrow(),
        [
            vec![range(row_key(1), &row_key(2))],
            vec![range(row_key(5), &row_key(6))],
        ]
    );
}

/// `GetExtraDBReaderByRegion` answering a region error leaves that region's
/// rows to the client.
#[test]
fn rows_of_an_unreadable_region_are_left_to_the_client() {
    let mut store = MvccStore::new();
    seed_int(&mut store, &[1, 2, 3, 4]);
    let mut regions = RegionStore::split(&store, vec![mvcc_key(&row_key(3))], vec![true, true]);
    regions.unreadable[0] = true;
    let (rows, left) = run(&regions, index_look_up(false));
    assert_eq!(rows, table_rows(&[3, 4]));
    assert_eq!(left, index_rows(&[1, 2]));
}

/// The index order is not the handle order: the handles are looked up
/// sorted (`slices.SortFunc(sortedHandles, ...)`), the left index rows keep
/// the index order.
#[test]
fn handles_are_looked_up_in_handle_order() {
    let mut store = MvccStore::new();
    for a in 1..=5_i64 {
        let value = tidb_codec::encode_value(&[Datum::Int(2), Datum::Int(-a)]).expect("row");
        put(&mut store, row_key(a), value);
        seed_int_index(&mut store, a, -a);
    }
    let regions = RegionStore::split(&store, vec![mvcc_key(&row_key(4))], vec![true, false]);
    let (rows, left) = run(&regions, index_look_up(false));
    let row = |a: i64| vec![Datum::Int(a), Datum::Int(-a)];
    assert_eq!(rows, [row(1), row(2), row(3)]);
    assert_eq!(left, [vec![Datum::Int(-5), Datum::Int(5)], vec![Datum::Int(-4), Datum::Int(4)]]);
    assert_eq!(*regions.read_ranges.borrow(), [vec![range(row_key(1), &row_key(3))]]);
}

/// More index rows than one batch: every `next` looks a batch up, and the
/// response carries them all.
#[test]
fn a_lookup_runs_batch_by_batch() {
    let mut store = MvccStore::new();
    let handles = (1..=100).collect::<Vec<_>>();
    seed_int(&mut store, &handles);
    let regions = RegionStore::split(&store, vec![mvcc_key(&row_key(41))], vec![true, false]);
    let (rows, left) = run(&regions, index_look_up(false));
    assert_eq!(rows, table_rows(&handles[..40]));
    assert_eq!(left, index_rows(&handles[40..]));
    // 32 rows a batch: 1..=32, then 33..=40 before the region elsewhere.
    assert_eq!(
        *regions.read_ranges.borrow(),
        [
            vec![range(row_key(1), &row_key(32))],
            vec![range(row_key(33), &row_key(40))],
        ]
    );
}

/// A clustered table: the index scan hands the encoded common handle up in
/// an extra column (`commonHandleKeyIdx`), the lookup reads one range per
/// handle, and the left index rows keep only the channel's output offsets.
#[test]
fn a_common_handle_lookup_reads_one_range_per_handle() {
    let mut store = MvccStore::new();
    seed_common(&mut store, &[("x", 1), ("y", 2), ("z", 3)]);
    let key = |a: &str| encode_row_key(TABLE_ID, &common_handle(a));
    let regions = RegionStore::split(&store, vec![mvcc_key(&key("z"))], vec![true, false]);
    let (rows, left) = run(&regions, index_look_up(true));
    let bytes = |a: &str| Datum::Bytes(a.as_bytes().to_vec());
    assert_eq!(rows, [vec![bytes("x"), Datum::Int(1)], vec![bytes("y"), Datum::Int(2)]]);
    assert_eq!(left, [vec![Datum::Int(3), bytes("z")]]);
    assert_eq!(
        *regions.read_ranges.borrow(),
        [vec![range(key("x"), &key("x")), range(key("y"), &key("y"))]]
    );
}

/// The rows left to the client are the index scan's, typed as the index
/// scan's: its last column is the encoded common handle the lookup reads by.
#[test]
fn rows_left_to_the_client_carry_the_index_scans_columns() {
    let mut store = MvccStore::new();
    seed_common(&mut store, &[("x", 1)]);
    let regions = RegionStore::split(&store, Vec::new(), vec![false]);
    let mut req = request(index_look_up(true));
    let mut dag = tipb::DagRequest::decode(req.data.as_ref()).unwrap();
    dag.intermediate_output_channels[0].output_offsets = vec![0, 1, 2];
    dag.encode_type = Some(tipb::EncodeType::TypeChunk as i32);
    req.data = dag.encode_to_vec().into();
    let resp = handle_cop_request(&regions, &req);
    assert!(resp.other_error.is_empty(), "{}", resp.other_error);
    let select = tipb::SelectResponse::decode(resp.data.as_ref()).unwrap();
    assert_eq!(select.error, None);
    let output = &select.intermediate_outputs[0];
    assert_eq!(output.encode_type(), tipb::EncodeType::TypeChunk);
    let varchar = || FieldType::new(tidb_datatype::FieldTypeCode::Varchar);
    let fields = [FieldType::new(tidb_datatype::FieldTypeCode::Long), varchar(), varchar()];
    let (chunk, rest) = tidb_chunk::codec::Codec::new(fields.to_vec()).decode(output.chunks[0].rows_data());
    assert!(rest.is_empty());
    assert_eq!(chunk.num_rows(), 1);
    let row = chunk.get_row(0);
    assert_eq!(row.get_int64(0), 1);
    assert_eq!(row.get_bytes(1), b"x");
    assert_eq!(row.get_bytes(2), common_handle("x"));
}

/// Only an index scan under a lookup returns the common handle key: Go's
/// `inBuildIndexLookUp`.
#[test]
fn a_plain_index_scan_returns_no_common_handle_key() {
    let mut store = MvccStore::new();
    seed_common(&mut store, &[("x", 1)]);
    let regions = RegionStore::one_region(&store);
    let mut lookup = index_look_up(true);
    let index_scan = lookup.index_lookup.as_mut().unwrap().children.remove(0);
    let mut req = request(index_scan);
    let mut dag = tipb::DagRequest::decode(req.data.as_ref()).unwrap();
    dag.intermediate_output_channels.clear();
    req.data = dag.encode_to_vec().into();
    let resp = handle_cop_request(&regions, &req);
    assert!(resp.other_error.is_empty(), "{}", resp.other_error);
    let select = tipb::SelectResponse::decode(resp.data.as_ref()).unwrap();
    assert_eq!(select.error, None);
    assert_eq!(decode_rows(&select.chunks), [vec![Datum::Int(1), Datum::Bytes(b"x".to_vec())]]);
}

/// An index entry without its row: Go panics on the count mismatch.
#[test]
#[should_panic(expected = "data may be inconsistency, expectCnt(3) != readCnt(2)")]
fn a_dangling_index_entry_panics_like_go() {
    let mut store = MvccStore::new();
    seed_int(&mut store, &[1, 2]);
    seed_int_index(&mut store, 3, 30);
    let regions = RegionStore::one_region(&store);
    run(&regions, index_look_up(false));
}

/// Go `buildIndexLookUp`'s three refusals.
#[test]
fn build_index_look_up_refuses_like_go() {
    let store = MvccStore::new();
    let regions = RegionStore::one_region(&store);
    let other_error = |root: tipb::Executor, paging_size: u64| {
        let mut req = request(root);
        req.paging_size = paging_size;
        handle_cop_request(&regions, &req).other_error
    };

    let mut one_child = index_look_up(false);
    one_child.index_lookup.as_mut().unwrap().children.pop();
    assert_eq!(other_error(one_child, 0), "IndexLookUp should have two children");

    let mut two_index_scans = index_look_up(false);
    let children = &mut two_index_scans.index_lookup.as_mut().unwrap().children;
    children[1] = children[0].clone();
    assert_eq!(
        other_error(two_index_scans, 0),
        "The second child of IndexLookUp should be TableScan"
    );

    assert_eq!(
        other_error(index_look_up(false), 1),
        "paging not supported in index lookup push down"
    );
}

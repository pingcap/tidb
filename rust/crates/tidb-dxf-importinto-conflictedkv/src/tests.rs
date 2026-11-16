// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `conflictedkv`'s `row_handle_test.go`, `handler_test.go`,
//! `deleter_test.go` and `collector_test.go`, over an in-memory store.
//!
//! `testdata/{tc,tn}.json` are the `TableInfo`s Go master creates for the
//! tests' tables (`create table tc(a bigint primary key clustered, b int,
//! c int, index(b), unique(c))`, `tn` the same with a nonclustered key),
//! read from a Go tidb-server's `/schema` API. `tf.json` is `create table
//! tf(id bigint primary key clustered, a int, unique key uk_expr ((a +
//! 1)))`, then `add column tail int` and `add unique key uk_tail (tail)`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex, RwLock};

use tidb_datatype::Datum;
use tidb_executor::importer::kv_encode::TableKVEncoder;
use tidb_executor::importer::{ASTArgs, LoadDataController, Plan};
use tidb_gocontext::Context;
use tidb_ingestor_simplesst::KVPair;
use tidb_lightning_backend_encode::EncodingConfig;
use tidb_lightning_backend_kv::{Pairs, PanickingAllocators};
use tidb_table::tables::TableCommon;
use tidb_tablecodec::table_key::RecordHandle;
use tidb_txnkv::kv_storage::{Storage, Transaction};
use tidb_txnkv::transaction::{
    BufferMutation, BufferMutationOp, CommittedTransaction, OptimisticCommitOutcome, OptimisticCoordinatorError,
    OptimisticTransactionReceipt, ReadOnlyTransaction,
};

use super::*;

type TxnResult<T> = std::result::Result<T, OptimisticCoordinatorError>;

/// A store over one map; a transaction reads the map as it began.
#[derive(Default)]
struct MemStore {
    data: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    ts: AtomicU64,
}

struct MemTxn {
    store: Arc<MemStore>,
    snapshot: BTreeMap<Vec<u8>, Vec<u8>>,
    start_ts: u64,
}

#[derive(Clone)]
struct SharedStore(Arc<MemStore>);

impl SharedStore {
    fn txn(&self) -> MemTxn {
        MemTxn {
            store: Arc::clone(&self.0),
            snapshot: self.0.data.lock().unwrap().clone(),
            start_ts: self.0.ts.fetch_add(1, Ordering::SeqCst) + 1,
        }
    }
}

impl Storage for SharedStore {
    fn begin(&self) -> TxnResult<Box<dyn Transaction>> {
        Ok(Box::new(self.txn()))
    }
    fn begin_read_only(&self) -> TxnResult<Box<dyn Transaction>> {
        Ok(Box::new(self.txn()))
    }
}

fn unsupported() -> OptimisticCoordinatorError {
    OptimisticCoordinatorError::SnapshotGet("unsupported".to_owned())
}

impl Transaction for MemTxn {
    fn start_ts(&self) -> u64 {
        self.start_ts
    }
    fn set_request_source(&mut self, _: &tidb_txnkv::RequestSource) {}
    fn set_snapshot_runtime_stats(&mut self, _: tidb_txnkv::SnapshotRuntimeStatsHandle) {}
    fn set_commit_details(&mut self, _: tikv_client::util::SharedCommitDetails) {}
    fn get(&mut self, key: &[u8], _: &tidb_txnkv::UnaryCallContext) -> TxnResult<Option<Vec<u8>>> {
        Ok(self.snapshot.get(key).cloned())
    }
    fn scan(
        &mut self,
        _: &[u8],
        _: &[u8],
        _: Option<usize>,
        _: &tidb_txnkv::UnaryCallContext,
    ) -> TxnResult<Vec<(Vec<u8>, Vec<u8>)>> {
        Err(unsupported())
    }
    fn scan_regions(
        &mut self,
        _: &[u8],
        _: &[u8],
        _: &tidb_txnkv::UnaryCallContext,
    ) -> TxnResult<Vec<tidb_txnkv::kv_storage::SnapshotScanRegion>> {
        Err(unsupported())
    }
    fn commit(
        self: Box<Self>,
        mutations: Vec<BufferMutation>,
        _: &tidb_txnkv::UnaryCallContext,
    ) -> TxnResult<OptimisticCommitOutcome> {
        let mut data = self.store.data.lock().unwrap();
        let count = mutations.len();
        for m in mutations {
            match m.kind() {
                BufferMutationOp::Delete => {
                    data.remove(m.key());
                }
                BufferMutationOp::Set => {
                    data.insert(m.key().to_vec(), m.value().to_vec());
                }
                BufferMutationOp::Lock => {}
            }
        }
        Ok(OptimisticCommitOutcome::Committed(CommittedTransaction {
            receipt: OptimisticTransactionReceipt::new(1, self.start_ts, Vec::new(), count),
            secondary_failures: Vec::new(),
        }))
    }
    fn finish_without_writes(self: Box<Self>) -> TxnResult<ReadOnlyTransaction> {
        Err(unsupported())
    }
}

fn table(name: &str) -> (tidb_model::TableInfo, TableCommon) {
    let json = match name {
        "tc" => include_str!("../testdata/tc.json"),
        "tn" => include_str!("../testdata/tn.json"),
        "tf" => include_str!("../testdata/tf.json"),
        _ => unreachable!(),
    };
    let meta = tidb_meta::value::parse_table_info(json.as_bytes(), 0).unwrap();
    let table =
        tidb_table::table_from_meta::table_from_meta(&meta, &tidb_table::table_from_meta::AutoIdSource::Unavailable).unwrap();
    (meta, table)
}

/// Go `getEncoder`.
fn encoder(meta: &tidb_model::TableInfo, table: &TableCommon) -> TableKVEncoder {
    let tidb_ast::Stmt::Dml(dml) = tidb_parser::parse("import into t from '/x.csv'").unwrap() else { unreachable!() };
    let tidb_ast::DmlStmt::ImportInto(stmt) = dml.as_ref() else { unreachable!() };
    let plan = Plan::new_import_plan(stmt, 1, "").unwrap();
    let ctrl = LoadDataController::new(plan, table.clone(), ASTArgs::default()).unwrap();
    let cfg = EncodingConfig {
        session_options: Default::default(),
        path: String::new(),
        table: meta.clone(),
        use_identity_auto_row_id: true,
    };
    TableKVEncoder::new_for_dup_resolve(&cfg, &ctrl, PanickingAllocators::new(meta.sep_auto_inc())).unwrap()
}

fn row_pairs(enc: &mut TableKVEncoder, id: i64, b: i64, c: i64) -> Pairs {
    enc.encode(&[Datum::Int(id), Datum::Int(b), Datum::Int(c)], id).unwrap()
}

fn index_id(key: &[u8]) -> Option<i64> {
    if tidb_tablecodec::is_record_key(key) {
        None
    } else {
        Some(tidb_tablecodec::table_key::decode_index_id(key).unwrap())
    }
}

/// Go `mockHandleEncodedRowFn`.
#[derive(Default)]
struct Recorder {
    rows: Vec<Vec<u8>>,
    kv_pairs: usize,
    /// The attempts left to fail.
    fail: usize,
}

impl EncodedRowHandler for Recorder {
    fn handle_encoded_row(&mut self, _: &Context, row_key: &[u8], _: &[Datum], pairs: &Pairs) -> Result<()> {
        if self.fail > 0 {
            self.fail -= 1;
            return Err(other("handle row"));
        }
        self.rows.push(row_key.to_vec());
        self.kv_pairs += pairs.pairs.len();
        Ok(())
    }
}

#[derive(Default)]
struct Progress(AtomicI64);

impl tidb_dxf_execute::Collector for Progress {
    fn accepted(&self, _: i64) {}
    fn processed(&self, units: i64, _: i64) {
        self.0.fetch_add(units, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct Traffic {
    read: AtomicU64,
    write: AtomicU64,
}

impl TrafficRecorder for Traffic {
    fn inc_cluster_read_bytes(&self, n: u64) {
        self.read.fetch_add(n, Ordering::SeqCst);
    }
    fn inc_cluster_write_bytes(&self, n: u64) {
        self.write.fetch_add(n, Ordering::SeqCst);
    }
}

fn receiver(pairs: Vec<KVPair>) -> PairReceiver {
    let (tx, rx) = sync_channel(pairs.len());
    for p in pairs {
        tx.send(p).unwrap();
    }
    Arc::new(Mutex::new(rx))
}

fn row_key(table_id: i64, h: i64) -> Vec<u8> {
    tidb_tablecodec::table_key::encode_row_key_with_handle(table_id, &RecordHandle::Int(h))
}

fn new_set(size: &Arc<AtomicI64>, limit: i64) -> BoundedKeySet {
    BoundedKeySet::new(Arc::clone(size), limit)
}

/// Go `TestKeyFilter`.
#[test]
fn key_filter() {
    let size = Arc::new(AtomicI64::new(0));
    let global = Arc::new(RwLock::new(new_set(&size, 1024)));
    global.write().unwrap().add(b"row-key-1");
    let mut filter = KeyFilter::new(global, new_set(&size, 1024));
    assert!(filter.is_handled_globally(b"row-key-1"));
    assert!(!filter.is_handled_globally(b"row-key-2"));
    assert!(!filter.is_handled_locally(b"row-key-2"));
    filter.add_local(b"row-key-2");
    assert!(filter.is_handled_locally(b"row-key-2"));
    assert!(!filter.is_handled_globally(b"row-key-2"));
}

/// Go `TestBoundedKeySet`.
#[test]
fn bounded_key_set() {
    // A partition handle's row key is under its partition.
    let p = |pid| {
        tidb_txnkv::Handle::Partition(tidb_txnkv::PartitionHandle::new(
            pid,
            Some(tidb_txnkv::Handle::Int(tidb_txnkv::IntHandle::new(1))),
        ))
    };
    let (k1, k2) = (handler::encode_data_row_key(100, &p(101)), handler::encode_data_row_key(100, &p(102)));
    assert_eq!(k1, row_key(101, 1));
    assert_eq!(k2, row_key(102, 1));
    // String keys honor the size limit.
    let size = Arc::new(AtomicI64::new(0));
    let mut set = new_set(&size, 1024);
    assert!(!set.contains(b"row-key"));
    set.add(b"row-key");
    assert!(set.contains(b"row-key"));
    assert_eq!(size.load(Ordering::SeqCst), 7 + 17);
    let exceeded_size = Arc::new(AtomicI64::new(0));
    let mut exceeded = new_set(&exceeded_size, 0);
    exceeded.add(b"row-key");
    assert!(exceeded.bound_exceeded());
    assert!(!exceeded.contains(b"row-key"));
    assert_eq!(exceeded_size.load(Ordering::SeqCst), 0);

    let size = Arc::new(AtomicI64::new(0));
    let limit = 3 * (1 + 17);
    let mut set = new_set(&size, limit);
    assert!(!set.contains(&[1]));
    for i in 1..=3u8 {
        set.add(&[i]);
        assert!(set.contains(&[i]));
    }
    assert_eq!(size.load(Ordering::SeqCst), limit);
    assert!(set.bound_exceeded());
    set.add(&[4]);
    assert!(!set.contains(&[4]));
    // A set over the shared size starts exceeded.
    let mut set2 = new_set(&size, limit);
    assert!(set2.bound_exceeded());
    set2.add(&[5]);
    assert!(!set2.contains(&[5]));
    set2.merge(set);
    for i in 1..=3u8 {
        assert!(set2.contains(&[i]));
    }
}

/// Go `TestHandler/test data kv handler`: the same row's data KV ten times
/// gives ten rows.
#[test]
fn data_kv_handler() {
    for (name, want_kvs) in [("tc", 30), ("tn", 40)] {
        let (meta, tbl) = table(name);
        let mut enc = encoder(&meta, &tbl);
        let record = row_pairs(&mut enc, 100, 100, 100).pairs.into_iter().find(|p| index_id(&p.key).is_none()).unwrap();
        let progress = Arc::new(Progress::default());
        let base = BaseHandler::new(tbl.clone(), "data", Some(encoder(&meta, &tbl)), Some(progress.clone()));
        let mut h = ConflictKVHandler::new_data(base, Recorder::default());
        h.pre_run().unwrap();
        let rx = receiver(vec![KVPair { key: record.key.clone(), value: record.val.clone() }; 10]);
        h.run(&Context::background(), &rx).unwrap();
        h.close(&Context::background()).unwrap();
        assert_eq!(h.row_handler.rows, vec![row_key(meta.id, 100); 10], "{name}");
        assert_eq!(h.row_handler.kv_pairs, want_kvs, "{name}");
        assert_eq!(progress.0.load(Ordering::SeqCst), 10, "{name}");
    }
}

/// Go `TestHandler/data kv handler re-encodes visible columns after
/// functional index`: the hidden column of the expression index is not
/// decoded, and the column added after it reads NULL, so the row re-encodes
/// to the KVs it was written with.
#[test]
fn data_kv_handler_visible_columns() {
    struct Check(BTreeMap<Vec<u8>, Vec<u8>>, bool);
    impl EncodedRowHandler for Check {
        fn handle_encoded_row(&mut self, _: &Context, _: &[u8], row: &[Datum], pairs: &Pairs) -> Result<()> {
            let got: BTreeMap<Vec<u8>, Vec<u8>> = pairs.pairs.iter().map(|p| (p.key.clone(), p.val.clone())).collect();
            assert_eq!(got, self.0);
            assert_eq!(row.len(), 3);
            assert!(row[2].is_null());
            self.1 = true;
            Ok(())
        }
    }
    let (meta, tbl) = table("tf");
    let fixture = encoder(&meta, &tbl).encode(&[Datum::Int(1), Datum::Int(10), Datum::Null], 1).unwrap();
    let want: BTreeMap<Vec<u8>, Vec<u8>> = fixture.pairs.iter().map(|p| (p.key.clone(), p.val.clone())).collect();
    // uk_tail=NULL is among them.
    assert!(fixture.pairs.iter().any(|p| index_id(&p.key) == Some(2)));
    let record = fixture.pairs.iter().find(|p| index_id(&p.key).is_none()).unwrap();
    let base = BaseHandler::new(tbl.clone(), "data", Some(encoder(&meta, &tbl)), None);
    let mut h = ConflictKVHandler::new_data(base, Check(want, false));
    h.handle(&Context::background(), &KVPair { key: record.key.clone(), value: record.val.clone() }).unwrap();
    assert!(h.row_handler.1);
}

/// Go `TestHandler/test index kv handler`: of 16 rows conflicting in pairs
/// on the unique index, rows 1-5 are in the store and 1 and 3 were handled
/// with another KV group, so 2, 4 and 5 are handled.
#[test]
fn index_kv_handler() {
    for (name, want_kvs) in [("tc", 9), ("tn", 12)] {
        let (meta, tbl) = table(name);
        let store = SharedStore(Arc::default());
        let mut enc = encoder(&meta, &tbl);
        for i in 1..=5 {
            for p in row_pairs(&mut enc, i, i, i).pairs {
                store.0.data.lock().unwrap().insert(p.key, p.val);
            }
        }
        let size = Arc::new(AtomicI64::new(0));
        let global = Arc::new(RwLock::new(new_set(&size, 1 << 20)));
        global.write().unwrap().add(&row_key(meta.id, 1));
        global.write().unwrap().add(&row_key(meta.id, 3));
        let progress = Arc::new(Progress::default());
        let traffic = Arc::new(Traffic::default());
        let base = BaseHandler::new(tbl.clone(), "2", Some(encoder(&meta, &tbl)), Some(progress.clone()));
        let snapshot = LazyRefreshedSnapshot::new(Arc::new(store), Some(traffic.clone()));
        let filter = KeyFilter::new(Arc::clone(&global), new_set(&size, 1 << 20));
        let mut h = ConflictKVHandler::new_index(base, snapshot, Some(filter), Recorder::default());
        h.pre_run().unwrap();
        let mut pairs = Vec::new();
        for id in 1..=16 {
            for p in row_pairs(&mut enc, id, id, (id + 1) / 2).pairs {
                if index_id(&p.key) == Some(2) {
                    pairs.push(KVPair { key: p.key, value: p.val });
                }
            }
        }
        h.run(&Context::background(), &receiver(pairs)).unwrap();
        h.close(&Context::background()).unwrap();
        assert!(traffic.read.load(Ordering::SeqCst) > 0);
        let mut rows = h.row_handler.rows.clone();
        rows.sort();
        assert_eq!(rows, [2, 4, 5].map(|i| row_key(meta.id, i)), "{name}");
        assert_eq!(h.row_handler.kv_pairs, want_kvs, "{name}");
        let local = h.take_key_filter().unwrap().into_local_set();
        for i in [2, 4, 5] {
            assert!(local.contains(&row_key(meta.id, i)));
        }
        for i in 6..=16 {
            assert!(!local.contains(&row_key(meta.id, i)));
        }
        assert_eq!(progress.0.load(Ordering::SeqCst), 16, "{name}");
    }
}

/// Go `TestHandler/index row is marked local only after successful
/// handling`: a failed row stays buffered and `close` retries it.
#[test]
fn index_row_marked_local_after_success() {
    let (meta, tbl) = table("tc");
    let store = SharedStore(Arc::default());
    let mut enc = encoder(&meta, &tbl);
    let mut index_kv = None;
    for p in row_pairs(&mut enc, 1, 1, 1).pairs {
        if index_id(&p.key) == Some(2) {
            index_kv = Some(KVPair { key: p.key.clone(), value: p.val.clone() });
        }
        store.0.data.lock().unwrap().insert(p.key, p.val);
    }
    let old = BUFFERED_HANDLE_LIMIT.swap(1, Ordering::SeqCst);
    let size = Arc::new(AtomicI64::new(0));
    let filter = KeyFilter::new(Arc::new(RwLock::new(new_set(&size, 1 << 20))), new_set(&size, 1 << 20));
    let base = BaseHandler::new(tbl.clone(), "2", Some(encoder(&meta, &tbl)), None);
    let snapshot = LazyRefreshedSnapshot::new(Arc::new(store), None);
    let mut h = ConflictKVHandler::new_index(base, snapshot, Some(filter), Recorder { fail: 1, ..Recorder::default() });
    h.pre_run().unwrap();
    let ctx = Context::background();
    assert_eq!(h.handle(&ctx, &index_kv.unwrap()).unwrap_err().to_string(), "handle row");
    h.close(&ctx).unwrap();
    BUFFERED_HANDLE_LIMIT.store(old, Ordering::SeqCst);
    assert_eq!(h.row_handler.rows, vec![row_key(meta.id, 1)]);
    assert!(h.take_key_filter().unwrap().into_local_set().contains(&row_key(meta.id, 1)));
}

/// Go `TestDeleter`: rows 1-10 in the store, the KVs of the first rows of
/// the KV group already gone, each conflicted KV sent twice. The deleter
/// removes every other KV of those rows.
#[test]
fn deleter() {
    for (kv_group, conflicted) in [("data", 7), ("2", 5)] {
        let (meta, tbl) = table("tc");
        let store = SharedStore(Arc::default());
        let mut enc = encoder(&meta, &tbl);
        let mut conflicted_kvs = Vec::new();
        let mut kept = BTreeMap::new();
        for i in 1..=10 {
            for p in row_pairs(&mut enc, i, i, i).pairs {
                let in_group = match kv_group {
                    "data" => index_id(&p.key).is_none(),
                    _ => index_id(&p.key) == Some(2),
                };
                if i <= conflicted && in_group {
                    // Removed by the import, recorded as conflicted.
                    conflicted_kvs.push(KVPair { key: p.key.clone(), value: p.val.clone() });
                    conflicted_kvs.push(KVPair { key: p.key, value: p.val });
                    continue;
                }
                if i > conflicted {
                    kept.insert(p.key.clone(), p.val.clone());
                }
                store.0.data.lock().unwrap().insert(p.key, p.val);
            }
        }
        let old = BUFFERED_KEY_COUNT_LIMIT.swap(2, Ordering::SeqCst);
        let traffic = Arc::new(Traffic::default());
        let storage: Arc<dyn Storage> = Arc::new(store.clone());
        let deleter = Deleter::new(tbl.clone(), storage, kv_group, encoder(&meta, &tbl), None, Some(traffic.clone()));
        deleter.run(&Context::background(), &receiver(conflicted_kvs)).unwrap();
        BUFFERED_KEY_COUNT_LIMIT.store(old, Ordering::SeqCst);
        assert_eq!(*store.0.data.lock().unwrap(), kept, "{kv_group}");
        assert!(traffic.read.load(Ordering::SeqCst) > 0);
        assert!(traffic.write.load(Ordering::SeqCst) > 0);
    }
}

/// Go `TestCollectResultMerge`.
#[test]
fn collect_result_merge() {
    let mut r = CollectResult::new(&[]);
    let mut other = CollectResult::new(&[]);
    other.row_count = 2;
    other.total_file_size = 10;
    other.row_recording_capped = true;
    other.filenames = vec!["a".to_owned()];
    other.checksum = tidb_lightning_verification::KvChecksum::make(1, 2, 3);
    r.merge(&other);
    r.merge(&other);
    assert_eq!((r.row_count, r.total_file_size, r.row_recording_capped), (4, 20, true));
    assert_eq!(r.filenames, ["a", "a"]);
    assert_eq!((r.checksum.sum_size(), r.checksum.sum_kvs(), r.checksum.sum()), (2, 4, 0));
}

fn local_store() -> (tempfile::TempDir, Arc<dyn tidb_objstore::storeapi::Storage>) {
    let dir = tempfile::tempdir().unwrap();
    let store = tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap();
    (dir, Arc::new(store))
}

/// Go `TestCollectorHandleEncodedRow` over the data KV group: each
/// conflicted row is counted, checksummed and written as Go's
/// `DatumsToString`, a line per row.
#[test]
fn collector_records_rows() {
    let (meta, tbl) = table("tc");
    let (dir, obj) = local_store();
    let mut enc = encoder(&meta, &tbl);
    let mut pairs = Vec::new();
    let mut want = tidb_lightning_verification::KvChecksum::with_keyspace(&[]);
    for i in 1..=3 {
        let row = row_pairs(&mut enc, i, i * 10, i * 100);
        want.update(&row.pairs);
        let rec = row.pairs.into_iter().find(|p| index_id(&p.key).is_none()).unwrap();
        pairs.push(KVPair { key: rec.key, value: rec.val });
    }
    let size = Arc::new(AtomicI64::new(0));
    let storage: Arc<dyn Storage> = Arc::new(SharedStore(Arc::default()));
    let mut c = Collector::new(
        tbl.clone(),
        obj,
        &storage,
        &[],
        "conflicted-rows/1/2-x".to_owned(),
        "data",
        encoder(&meta, &tbl),
        Arc::new(RwLock::new(new_set(&size, 1 << 20))),
        new_set(&size, 1 << 20),
        Arc::new(AtomicI64::new(0)),
        None,
        None,
    );
    let ctx = Context::background();
    c.run(&ctx, &receiver(pairs)).unwrap();
    c.close(&ctx).unwrap();
    let r = c.collect_result();
    assert_eq!(r.row_count, 3);
    assert_eq!(r.filenames, ["conflicted-rows/1/2-x/data-0001.txt"]);
    assert!(r.checksum == want);
    let text = std::fs::read_to_string(dir.path().join("conflicted-rows/1/2-x/data-0001.txt")).unwrap();
    assert_eq!(text, "(1, 10, 100)\n(2, 20, 200)\n(3, 30, 300)\n");
    assert_eq!(r.total_file_size, text.len() as i64);
    assert!(!r.row_recording_capped);
}

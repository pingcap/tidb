//! Go `br/pkg/backup` `client_test.go`, `limit_test.go` and
//! `store_test.go`: the parts that run without a TiKV cluster. Schema
//! building and DDL job collection read a real catalog and are covered by
//! the live BACKUP check.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use crossbeam_channel::{select, Sender};
use tidb_gocontext::Context;
use tidb_proto::backup::{self as backuppb, BackupRequest, BackupResponse};
use tidb_proto::{kvrpcpb, metapb};

use super::client::{
    check_backup_storage_is_locked, Client, ClientMgr, MainBackupLoop, ProgressUnit,
};
use super::limit::ResourceConcurrentLimiter;
use super::store::{
    split_backup_req_ranges, BackupClient, BackupRetryPolicy, BackupSender, ResponseAndStore,
};
use crate::errors::Result;
use crate::glue::Storage;
use crate::rtree::{KeyRange, ProgressRange, ProgressRangeTree, RangeTree};
use crate::test_util::MockPdClient;
use crate::utils::error_handling::ErrorContext;

fn runtime() -> Arc<tokio::runtime::Runtime> {
    static RT: std::sync::OnceLock<Arc<tokio::runtime::Runtime>> = std::sync::OnceLock::new();
    Arc::clone(RT.get_or_init(|| {
        Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap(),
        )
    }))
}

/// A client to nowhere: Go's mock answers a nil client.
fn lazy_client(addr: &str) -> BackupClient {
    let _guard = runtime().enter();
    let channel = tonic::transport::Endpoint::from_shared(addr.to_owned())
        .unwrap()
        .connect_lazy();
    BackupClient::new(channel)
}

/// Go `createBackupSuite`'s manager over the mock PD.
struct MockMgr {
    pd: Arc<MockPdClient>,
    gc: Arc<dyn crate::gc::Manager>,
}

impl MockMgr {
    fn new(pd: Arc<MockPdClient>) -> Arc<Self> {
        let gc = crate::gc::new_manager(
            Arc::clone(&pd) as Arc<dyn crate::pdclient::PdClient>,
            crate::gc::NULLSPACE_ID,
        );
        Arc::new(Self { pd, gc })
    }
}

impl ClientMgr for MockMgr {
    fn get_backup_client(&self, _: &Context, _: u64) -> Result<BackupClient> {
        Ok(lazy_client("http://127.0.0.1:1"))
    }
    fn reset_backup_client(&self, _: &Context, _: u64) -> Result<BackupClient> {
        Ok(lazy_client("http://127.0.0.1:1"))
    }
    fn get_pd_client(&self) -> Arc<dyn crate::pdclient::PdClient> {
        Arc::clone(&self.pd) as Arc<dyn crate::pdclient::PdClient>
    }
    fn get_storage(&self) -> Arc<dyn Storage> {
        unreachable!("the mock manager has no store")
    }
    fn get_gc_manager(&self) -> Arc<dyn crate::gc::Manager> {
        Arc::clone(&self.gc)
    }
    fn resolve_locks_for_read(
        &self,
        _: &Context,
        _: u64,
        _: Vec<kvrpcpb::LockInfo>,
    ) -> Result<(Vec<u64>, Vec<u64>)> {
        Ok((Vec::new(), Vec::new()))
    }
    fn runtime(&self) -> Arc<tokio::runtime::Runtime> {
        runtime()
    }
}

fn up_store(id: u64) -> metapb::Store {
    metapb::Store {
        id,
        address: format!("127.0.0.1:{}", 20159 + id),
        state: metapb::StoreState::Up as i32,
        ..Default::default()
    }
}

fn suite() -> (Arc<MockPdClient>, Client) {
    let pd = Arc::new(MockPdClient::default());
    *pd.ts.lock().unwrap() = Some((-1, 0));
    let client = Client::new(&Context::background(), MockMgr::new(Arc::clone(&pd)));
    (pd, client)
}

/// Go `TestGetTS`.
#[test]
fn get_ts() {
    let (pd, client) = suite();
    let ctx = Context::background();
    let now_ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    };
    let deviation = 100;

    let current = now_ms();
    let ts = client.get_ts(&ctx, 0, 0).unwrap();
    let d = current - crate::pdclient::extract_physical(ts);
    assert!(d > -deviation && d < deviation, "{d}");

    let current = now_ms();
    let ts = client.get_ts(&ctx, 90 * 1_000_000_000, 0).unwrap();
    let d = current - crate::pdclient::extract_physical(ts);
    assert!(d > 90_000 - deviation && d < 90_000 + deviation, "{d}");

    let err = client
        .get_ts(&ctx, -60 * 1_000_000_000, 0)
        .unwrap_err()
        .to_string();
    assert!(err.starts_with("negative timeago is not allowed"), "{err}");

    // Go's `1000000*time.Hour` overflows the physical part below zero.
    let err = client
        .get_ts(&ctx, 1_000_000 * 3_600 * 1_000_000_000, 0)
        .unwrap_err()
        .to_string();
    assert!(err.contains("backup ts overflow"), "{err}");

    let (p, l) = crate::pdclient::PdClient::get_ts(pd.as_ref(), &ctx).unwrap();
    *pd.gc_safe_point.lock().unwrap() = crate::pdclient::compose_ts(p, l);
    let err = client
        .get_ts(&ctx, 10 * 3_600 * 1_000_000_000, 0)
        .unwrap_err()
        .to_string();
    assert!(
        regex::Regex::new("GC safepoint [0-9]+ exceed TS [0-9]+")
            .unwrap()
            .is_match(&err),
        "{err}"
    );

    let (p, l) = crate::pdclient::PdClient::get_ts(pd.as_ref(), &ctx).unwrap();
    let backup_ts = crate::pdclient::compose_ts(p, l);
    assert_eq!(
        client.get_ts(&ctx, 60 * 1_000_000_000, backup_ts).unwrap(),
        backup_ts
    );

    let future = crate::pdclient::compose_ts(now_ms() + 60_000, 0);
    let err = client.get_ts(&ctx, 0, future).unwrap_err().to_string();
    assert!(
        err.contains("must not be later than current timestamp"),
        "{err}"
    );
}

/// Go `TestCheckBackupIsLocked`.
#[test]
fn check_backup_is_locked() {
    let dir = tempfile::tempdir().unwrap();
    let s: crate::metautil::Storage =
        Arc::new(tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
    let ctx = Context::background();
    check_backup_storage_is_locked(&ctx, &s).unwrap();
    s.write_file(&ctx, crate::metautil::LOCK_FILE, b"").unwrap();
    check_backup_storage_is_locked(&ctx, &s).unwrap();
    s.write_file(&ctx, "1.txt", b"").unwrap();
    check_backup_storage_is_locked(&ctx, &s).unwrap();
    s.write_file(&ctx, "1.sst", b"").unwrap();
    let err = check_backup_storage_is_locked(&ctx, &s)
        .unwrap_err()
        .to_string();
    assert!(
        regex::Regex::new("backup lock file and sst file exist in(.+)")
            .unwrap()
            .is_match(&err),
        "{err}"
    );
}

fn progress_range(start: &[u8], end: &[u8]) -> Arc<Mutex<ProgressRange>> {
    Arc::new(Mutex::new(ProgressRange {
        res: RangeTree::new(),
        origin: KeyRange::new(start, end),
    }))
}

/// Go `TestOnBackupResponse`.
#[test]
fn on_backup_response() {
    let (_, client) = suite();
    let ctx = Context::background();
    let err_context = ErrorContext::new("test", 1);
    let tree = Mutex::new(ProgressRangeTree::new(None, false));
    let error_resp = || ResponseAndStore {
        store_id: 0,
        resp: BackupResponse {
            error: Some(backuppb::Error {
                msg: "test".into(),
                detail: None,
            }),
            ..Default::default()
        },
    };
    // The first error is retried within the error context, the second not.
    assert!(client
        .on_backup_response(&ctx, error_resp(), &err_context, &tree)
        .unwrap()
        .is_none());
    assert!(client
        .on_backup_response(&ctx, error_resp(), &err_context, &tree)
        .is_err());

    let resp = |start: &[u8], end: &[u8]| ResponseAndStore {
        store_id: 0,
        resp: BackupResponse {
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            ..Default::default()
        },
    };
    tree.lock()
        .unwrap()
        .insert(progress_range(b"aa", b"c"))
        .unwrap();
    // The response range is outside the tree's: a duplicated response.
    assert!(client
        .on_backup_response(&ctx, resp(b"a", b"b"), &err_context, &tree)
        .unwrap()
        .is_none());
    // A part of the range.
    assert!(client
        .on_backup_response(&ctx, resp(b"aa", b"b"), &err_context, &tree)
        .unwrap()
        .is_none());
    let incomplete = tree.lock().unwrap().get_incomplete_ranges().unwrap();
    assert_eq!(incomplete.len(), 1);
    assert_eq!(
        (
            incomplete[0].start_key.as_slice(),
            incomplete[0].end_key.as_slice()
        ),
        (&b"b"[..], &b"c"[..])
    );
    // The rest.
    assert!(client
        .on_backup_response(&ctx, resp(b"b", b"c"), &err_context, &tree)
        .unwrap()
        .is_none());
    assert!(tree
        .lock()
        .unwrap()
        .get_incomplete_ranges()
        .unwrap()
        .is_empty());

    // A locked key answers the lock.
    let locked = ResponseAndStore {
        store_id: 0,
        resp: BackupResponse {
            error: Some(backuppb::Error {
                msg: String::new(),
                detail: Some(backuppb::error::Detail::KvError(kvrpcpb::KeyError {
                    locked: Some(kvrpcpb::LockInfo {
                        primary_lock: b"b".to_vec(),
                        key: b"b".to_vec(),
                        lock_ttl: 50,
                        txn_size: 1,
                        lock_type: kvrpcpb::Op::Put as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        },
    };
    let lock = client
        .on_backup_response(&ctx, locked, &err_context, &tree)
        .unwrap()
        .unwrap();
    assert_eq!(lock.primary_lock, b"b");
}

/// Go `TestBuildProgressRangeTree`.
#[test]
fn build_progress_range_tree() {
    let (_, client) = suite();
    let ranges = [
        KeyRange::new(&b"aa"[..], &b"b"[..]),
        KeyRange::new(&b"c"[..], &b"d"[..]),
        KeyRange::new(&b"f"[..], &b"g"[..]),
    ];
    let tree = client
        .build_progress_range_tree(
            &Context::background(),
            &ranges,
            None,
            Arc::new(|_: ProgressUnit| {}),
        )
        .unwrap();
    let contained = |s: &[u8], e: &[u8]| tree.find_contained(s, e);
    assert!(contained(b"a", b"aa").unwrap().is_none());
    assert!(contained(b"b", b"ba").unwrap().is_none());
    assert!(contained(b"e", b"ea").unwrap().is_none());
    let found = contained(b"aa", b"b").unwrap().unwrap();
    assert_eq!(
        found.lock().unwrap().origin,
        KeyRange::new(&b"aa"[..], &b"b"[..])
    );
    assert!(contained(b"cc", b"e").is_err());
    assert!(contained(b"e", b"ff").unwrap().is_none());
}

/// Go `genSubRanges`.
fn gen_sub_ranges(req: &mut BackupRequest, count: usize) {
    for i in 0..count {
        req.sub_ranges.push(kvrpcpb::KeyRange {
            start_key: vec![i as u8],
            end_key: vec![i as u8 + 1],
        });
    }
}

/// Go `TestSplitBackupReqRanges`.
#[test]
fn split_backup_req_ranges_cases() {
    let mut req = BackupRequest::default();
    assert_eq!(split_backup_req_ranges(req.clone(), 1).len(), 1);
    assert_eq!(split_backup_req_ranges(req.clone(), 0).len(), 1);
    gen_sub_ranges(&mut req, 10);
    for n in [10, 11] {
        let res = split_backup_req_ranges(req.clone(), n);
        assert_eq!(res.len(), 10);
        for (r, sub) in res.iter().zip(&req.sub_ranges) {
            assert_eq!(r.sub_ranges[0], *sub);
        }
    }
    let res = split_backup_req_ranges(req.clone(), 9);
    assert_eq!(res.len(), 9);
    assert_eq!(res[0].sub_ranges, req.sub_ranges[0..2]);
    for i in 2..10 {
        assert_eq!(res[i - 1].sub_ranges[0], req.sub_ranges[i]);
    }
    let res = split_backup_req_ranges(req.clone(), 3);
    assert_eq!(res.len(), 3);
    assert_eq!(res[0].sub_ranges, req.sub_ranges[0..4]);
    for (i, r) in res.iter().enumerate().skip(1) {
        assert_eq!(r.sub_ranges, req.sub_ranges[i * 3 + 1..i * 3 + 4]);
    }
}

/// Go `TestSplitBackupReqRanges2`.
#[test]
fn split_backup_req_ranges_lengths() {
    let cases: &[(usize, usize, &[usize])] = &[
        (8, 0, &[8]),
        (8, 1, &[8]),
        (8, 2, &[4, 4]),
        (8, 3, &[3, 3, 2]),
        (8, 4, &[2, 2, 2, 2]),
        (8, 5, &[2, 2, 2, 1, 1]),
        (8, 6, &[2, 2, 1, 1, 1, 1]),
        (8, 7, &[2, 1, 1, 1, 1, 1, 1]),
        (8, 8, &[1, 1, 1, 1, 1, 1, 1, 1]),
        (8, 9, &[1, 1, 1, 1, 1, 1, 1, 1]),
        (8, 1024, &[1, 1, 1, 1, 1, 1, 1, 1]),
        (73, 13, &[6, 6, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5]),
    ];
    for &(total, n, lens) in cases {
        let mut req = BackupRequest::default();
        gen_sub_ranges(&mut req, total);
        let res = split_backup_req_ranges(req.clone(), n);
        let mut index = 0;
        for (i, &l) in lens.iter().enumerate() {
            assert_eq!(res[i].sub_ranges.len(), l, "{total}/{n}");
            for r in &res[i].sub_ranges {
                assert_eq!(*r, req.sub_ranges[index]);
                index += 1;
            }
        }
    }
}

/// Go `TestResourceConcurrentLimiter`.
#[test]
fn resource_concurrent_limiter() {
    let limiter = Arc::new(ResourceConcurrentLimiter::new(100));
    let tag = Arc::new(AtomicI64::new(0));
    limiter.acquire(50);
    limiter.acquire(50);
    let (l, t) = (Arc::clone(&limiter), Arc::clone(&tag));
    let h = std::thread::spawn(move || {
        l.acquire(200);
        assert_eq!(t.load(Ordering::SeqCst), 1);
        assert_eq!(t.fetch_add(1, Ordering::SeqCst) + 1, 2);
        l.release(200);
    });
    assert_eq!(tag.fetch_add(1, Ordering::SeqCst) + 1, 1);
    limiter.release(50);
    limiter.release(50);
    h.join().unwrap();
    assert_eq!(tag.load(Ordering::SeqCst), 2);
    limiter.acquire(99);
    limiter.acquire(1);
    limiter.release(100);
}

/// Go `TestResourceConcurrentLimiter2`.
#[test]
fn resource_concurrent_limiter_bound() {
    let limiter = Arc::new(ResourceConcurrentLimiter::new(100));
    let hs: Vec<_> = (0..20)
        .map(|_| {
            let l = Arc::clone(&limiter);
            std::thread::spawn(move || {
                let current = l.acquire(30);
                std::thread::sleep(Duration::from_millis(10));
                l.release(30);
                assert!(current <= 120, "{current}");
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
}

/// Go `mockBackupBackupSender`: each store answers its responses once they
/// are there.
#[derive(Clone, Default)]
struct MockSender {
    responses: Arc<Mutex<HashMap<u64, Vec<ResponseAndStore>>>>,
    after_load: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

impl BackupSender for MockSender {
    fn send_async(
        &self,
        ctx: &Context,
        _: u64,
        store_id: u64,
        _: Arc<ResourceConcurrentLimiter>,
        _: BackupRequest,
        _: usize,
        _: BackupClient,
        resp_tx: Sender<ResponseAndStore>,
        _: Sender<BackupRetryPolicy>,
    ) {
        let (ctx, me) = (ctx.clone(), self.clone());
        std::thread::spawn(move || {
            let mut resps = me
                .responses
                .lock()
                .unwrap()
                .get(&store_id)
                .cloned()
                .unwrap_or_default();
            while resps.is_empty() {
                std::thread::sleep(Duration::from_millis(100));
                resps = me
                    .responses
                    .lock()
                    .unwrap()
                    .get(&store_id)
                    .cloned()
                    .unwrap_or_default();
            }
            if let Some(f) = &me.after_load {
                f(store_id);
            }
            for r in resps {
                select! {
                    recv(ctx.done()) -> _ => return,
                    send(resp_tx, r) -> _ => {}
                }
            }
        });
    }
}

/// The keys splitting `[aaa, zzz)` at ten random points.
fn split_keys() -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = (0..10)
        .map(|_| loop {
            let k: Vec<u8> = (0..3).map(|_| rand_byte()).collect();
            if k.as_slice() > &b"aaa"[..] && k.as_slice() < &b"zzz"[..] {
                break k;
            }
        })
        .collect();
    keys.sort();
    keys.dedup();
    let mut out = vec![b"aaa".to_vec()];
    out.extend(keys);
    out.push(b"zzz".to_vec());
    out
}

fn rand_byte() -> u8 {
    let mut b = [0u8; 1];
    getrandom::fill(&mut b).unwrap();
    b[0]
}

/// The responses for `keys`, alternating stores 1 and 2; `skip_last`
/// leaves the last range out.
fn responses(keys: &[Vec<u8>], skip_last: bool) -> HashMap<u64, Vec<ResponseAndStore>> {
    let mut out: HashMap<u64, Vec<ResponseAndStore>> = HashMap::new();
    let n = if skip_last {
        keys.len() - 2
    } else {
        keys.len() - 1
    };
    for i in 0..n {
        let store_id = [1, 2][i % 2];
        out.entry(store_id).or_default().push(ResponseAndStore {
            store_id,
            resp: BackupResponse {
                start_key: keys[i].clone(),
                end_key: keys[i + 1].clone(),
                ..Default::default()
            },
        });
    }
    out
}

struct LoopCase {
    connected: Arc<Mutex<HashMap<u64, i32>>>,
}

impl LoopCase {
    fn new() -> Self {
        Self {
            connected: Arc::default(),
        }
    }

    fn main_loop(
        &self,
        client: &Client,
        sender: MockSender,
        notifier: &(
            Sender<BackupRetryPolicy>,
            crossbeam_channel::Receiver<BackupRetryPolicy>,
        ),
    ) -> MainBackupLoop {
        let connected = Arc::clone(&self.connected);
        MainBackupLoop {
            sender: Arc::new(sender),
            backup_req: BackupRequest::default(),
            concurrency: 0,
            global_progress_tree: Mutex::new(
                client
                    .build_progress_range_tree(
                        &Context::background(),
                        &[KeyRange::new(&b"aaa"[..], &b"zzz"[..])],
                        None,
                        Arc::new(|_: ProgressUnit| {}),
                    )
                    .unwrap(),
            ),
            replica_read_label: BTreeMap::new(),
            state_notifier: notifier.clone(),
            limiter: Arc::new(ResourceConcurrentLimiter::new(1 << 20)),
            progress_call_back: Arc::new(|_: ProgressUnit| {}),
            get_backup_client_call_back: Arc::new(move |_, store_id, _| {
                *connected.lock().unwrap().entry(store_id).or_default() += 1;
                Ok(lazy_client("http://127.0.0.1:1"))
            }),
        }
    }

    fn connected(&self, store_id: u64) -> i32 {
        self.connected
            .lock()
            .unwrap()
            .get(&store_id)
            .copied()
            .unwrap_or(0)
    }
}

/// Go `TestMainBackupLoop`.
#[test]
fn main_backup_loop() {
    let (pd, client) = suite();
    *pd.stores.lock().unwrap() = vec![up_store(1), up_store(2)];
    let notifier = crossbeam_channel::bounded::<BackupRetryPolicy>(0);
    let bg = Context::background();
    let set_state = |id: u64, state: metapb::StoreState| {
        pd.stores
            .lock()
            .unwrap()
            .iter_mut()
            .find(|s| s.id == id)
            .unwrap()
            .state = state as i32;
    };

    // Case #1: an offline store is still backed up.
    let case = LoopCase::new();
    let sender = MockSender {
        responses: Arc::new(Mutex::new(responses(&split_keys(), false))),
        after_load: None,
    };
    set_state(1, metapb::StoreState::Offline);
    let mut l = case.main_loop(&client, sender, &notifier);
    client.run_loop(&bg, &mut l).unwrap();

    // Case #2: cancelled with a range left.
    let case = LoopCase::new();
    let sender = MockSender {
        responses: Arc::new(Mutex::new(responses(&split_keys(), true))),
        after_load: None,
    };
    let mut l = case.main_loop(&client, sender, &notifier);
    let (ctx, cancel) = Context::with_cancel(&bg);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        cancel.cancel();
    });
    assert!(client.run_loop(&ctx, &mut l).is_err());

    // Case #3: a store drops and never comes back; its regions move to the
    // other store once that one has loaded its first responses.
    let case = LoopCase::new();
    let mut resps = responses(&split_keys(), false);
    set_state(2, metapb::StoreState::Tombstone);
    let dropped = resps.insert(2, Vec::new()).unwrap();
    let shared = Arc::new(Mutex::new(resps));
    let first_load = Arc::new(Once::new());
    let (s2, fl) = (Arc::clone(&shared), Arc::clone(&first_load));
    let sender = MockSender {
        responses: Arc::clone(&shared),
        after_load: Some(Arc::new(move |store_id| {
            if store_id == 1 {
                let s2 = Arc::clone(&s2);
                let dropped = dropped.clone();
                fl.call_once(move || {
                    std::thread::spawn(move || {
                        s2.lock().unwrap().get_mut(&1).unwrap().extend(dropped)
                    });
                });
            }
        })),
    };
    let mut l = case.main_loop(&client, sender, &notifier);
    client.run_loop(&bg, &mut l).unwrap();
    assert!(case.connected(1) > 1);
    assert_eq!(case.connected(2), 0);

    // Case #4: a store drops and comes back soon.
    let case = LoopCase::new();
    let sender = MockSender {
        responses: Arc::new(Mutex::new(responses(&split_keys(), false))),
        after_load: None,
    };
    let mut l = case.main_loop(&client, sender, &notifier);
    let pd2 = Arc::clone(&pd);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(500));
        pd2.stores
            .lock()
            .unwrap()
            .iter_mut()
            .find(|s| s.id == 2)
            .unwrap()
            .state = metapb::StoreState::Up as i32;
    });
    client.run_loop(&bg, &mut l).unwrap();
    assert!(case.connected(1) > 1);
    assert_eq!(case.connected(2), 1);

    // Case #5: a store answers nothing for a while; the round waits for it.
    let case = LoopCase::new();
    let mut resps = responses(&split_keys(), false);
    let dropped = resps.insert(2, Vec::new()).unwrap();
    let shared = Arc::new(Mutex::new(resps));
    let s2 = Arc::clone(&shared);
    let sender = MockSender {
        responses: shared,
        after_load: None,
    };
    let mut l = case.main_loop(&client, sender, &notifier);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(500));
        s2.lock().unwrap().insert(2, dropped);
    });
    client.run_loop(&bg, &mut l).unwrap();
    assert_eq!(case.connected(1), 1);
    assert_eq!(case.connected(2), 1);
}

/// Go `TestObserveStoreChangesAsync`.
#[test]
fn observe_store_changes_async() {
    let pd = Arc::new(MockPdClient::with_stores(vec![up_store(1), up_store(2)]));
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/br/pkg/backup/backup-store-change-tick",
        "return(true)",
    );
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let (tx, rx) = crossbeam_channel::bounded::<BackupRetryPolicy>(1);
    super::store::observe_store_changes_async(
        &ctx,
        tx,
        Arc::clone(&pd) as Arc<dyn crate::pdclient::PdClient>,
    );
    // Nothing happens.
    assert!(rx.recv_timeout(Duration::from_secs(1)).is_err());
    // A store joins.
    pd.stores.lock().unwrap().push(up_store(3));
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        BackupRetryPolicy { one: 3, all: false }
    );
    // A store disconnects.
    pd.stores.lock().unwrap()[1].state = metapb::StoreState::Offline as i32;
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    let mut all = false;
    while std::time::Instant::now() < deadline && !all {
        all = rx
            .recv_timeout(Duration::from_millis(100))
            .is_ok_and(|p| p.all);
    }
    assert!(all);
    cancel.cancel();
}

/// A TiKV backup service whose `backup` stream never answers.
struct SilentBackup;

type Stream<T> = std::pin::Pin<
    Box<
        dyn tonic::codegen::tokio_stream::Stream<Item = std::result::Result<T, tonic::Status>>
            + Send,
    >,
>;

#[tonic::async_trait]
impl backuppb::backup_server::Backup for SilentBackup {
    type backupStream = Stream<BackupResponse>;
    type CheckPendingAdminOpStream = Stream<backuppb::CheckAdminResponse>;
    type PrepareSnapshotBackupStream = Stream<backuppb::PrepareSnapshotBackupResponse>;

    async fn backup(
        &self,
        _: tonic::Request<BackupRequest>,
    ) -> std::result::Result<tonic::Response<Self::backupStream>, tonic::Status> {
        Ok(tonic::Response::new(Box::pin(
            tonic::codegen::tokio_stream::pending(),
        )))
    }
    async fn check_pending_admin_op(
        &self,
        _: tonic::Request<backuppb::CheckAdminRequest>,
    ) -> std::result::Result<tonic::Response<Self::CheckPendingAdminOpStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    async fn prepare_snapshot_backup(
        &self,
        _: tonic::Request<tonic::Streaming<backuppb::PrepareSnapshotBackupRequest>>,
    ) -> std::result::Result<tonic::Response<Self::PrepareSnapshotBackupStream>, tonic::Status>
    {
        Err(tonic::Status::unimplemented(""))
    }
    async fn prepare(
        &self,
        _: tonic::Request<backuppb::PrepareRequest>,
    ) -> std::result::Result<tonic::Response<backuppb::PrepareResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    async fn cleanup(
        &self,
        _: tonic::Request<backuppb::CleanupRequest>,
    ) -> std::result::Result<tonic::Response<backuppb::CleanupResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
}

/// Go `TestTimeoutRecv`'s first case: a store that never answers is
/// cancelled with the response timeout.
#[test]
fn timeout_recv() {
    let rt = runtime();
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    rt.spawn(
        tonic::transport::Server::builder()
            .add_service(backuppb::backup_server::BackupServer::new(SilentBackup))
            .serve(addr),
    );
    std::thread::sleep(Duration::from_millis(200));
    let origin = *super::store::TIMEOUT_ONE_RESPONSE.lock().unwrap();
    *super::store::TIMEOUT_ONE_RESPONSE.lock().unwrap() = Duration::from_millis(800);
    let (tx, _rx) = crossbeam_channel::unbounded();
    let start = std::time::Instant::now();
    let err = super::store::start_backup(
        &Context::background(),
        &rt,
        0,
        &ResourceConcurrentLimiter::new(100),
        BackupRequest::default(),
        lazy_client(&format!("http://{addr}")),
        1,
        &tx,
    )
    .unwrap_err();
    *super::store::TIMEOUT_ONE_RESPONSE.lock().unwrap() = origin;
    assert_eq!(err.to_string(), "receive a backup response timeout");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "{:?}",
        start.elapsed()
    );
}

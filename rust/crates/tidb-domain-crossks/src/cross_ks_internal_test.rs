// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/domain/crossks/cross_ks_internal_test.go`.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tidb_syssession::{AdvancedSessionPool, SessionContext};

type C = dyn SessionContext;
type TestManager = Manager<C>;

struct TestSessionManager {
    ks: String,
    pool: Arc<dyn tidb_syssession::Pool<C>>,
    pool_close_count: AtomicUsize,
    store_close_count: AtomicUsize,
    on_close: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}
impl TestSessionManager {
    fn new(ks: &str) -> Arc<Self> {
        Arc::new(Self {
            ks: ks.into(),
            pool: Arc::new(AdvancedSessionPool::<C>::new(5, || {
                panic!("unused factory")
            })),
            pool_close_count: AtomicUsize::new(0),
            store_close_count: AtomicUsize::new(0),
            on_close: Mutex::new(None),
        })
    }
}
impl Runtime<C> for TestSessionManager {
    fn store_keyspace(&self) -> String {
        self.ks.clone()
    }
    fn sys_session_pool(&self) -> Arc<dyn tidb_syssession::Pool<C>> {
        Arc::clone(&self.pool)
    }
}
impl SessionManager<C> for TestSessionManager {
    fn close(&self) {
        self.pool.close();
        self.pool_close_count.fetch_add(1, Ordering::SeqCst);
        if let Some(on_close) = self.on_close.lock().unwrap().as_ref() {
            on_close();
        }
        self.store_close_count.fetch_add(1, Ordering::SeqCst);
    }
}
fn unused(_: &str) -> Result<Arc<dyn SessionManager<C>>, String> {
    panic!("test should use the pre-seeded runtime entry")
}
fn new_runtime_handle_test_manager(ks: &str) -> (Arc<TestManager>, Arc<TestSessionManager>) {
    let manager = Arc::new(Manager::new("SYSTEM".into()));
    let session = TestSessionManager::new(ks);
    manager.runtimes.lock().unwrap().insert(
        ks.into(),
        RuntimeEntry {
            sess_mgr: session.clone(),
            active_holders: HashSet::new(),
            last_release_at: None,
        },
    );
    (manager, session)
}
fn expire(manager: &TestManager, ks: &str) {
    manager
        .runtimes
        .lock()
        .unwrap()
        .get_mut(ks)
        .unwrap()
        .last_release_at =
        Some(Instant::now() - CROSS_KS_RUNTIME_IDLE_TIMEOUT - Duration::from_secs(1));
}

/// Go `TestAcquireRuntimeHandle`, including all four subtests.
#[test]
fn test_acquire_runtime_handle() {
    let (manager, session) = new_runtime_handle_test_manager("target");
    assert!(manager
        .acquire("target", "", unused)
        .err()
        .unwrap()
        .contains("holderID"));
    if tidb_config_kerneltype::is_classic() {
        assert!(manager
            .acquire("target", "holder", unused)
            .err()
            .unwrap()
            .contains("classic kernel or current keyspace"));
        return;
    }
    let first = manager.acquire("target", "holder-1", unused).unwrap();
    assert_eq!(first.store_keyspace(), "target");
    assert!(Arc::ptr_eq(&session.pool, &first.sys_session_pool()));
    assert!(manager.runtimes.lock().unwrap()["target"]
        .active_holders
        .contains("holder-1"));
    assert!(manager
        .acquire("target", "holder-1", unused)
        .err()
        .unwrap()
        .contains("already acquired"));
    let second = manager.acquire("target", "holder-2", unused).unwrap();
    first.release();
    first.release();
    {
        let entries = manager.runtimes.lock().unwrap();
        assert_eq!(
            entries["target"].active_holders,
            HashSet::from(["holder-2".into()])
        );
        assert!(entries["target"].last_release_at.is_none());
    }
    second.release();
    second.release();
    {
        let entries = manager.runtimes.lock().unwrap();
        assert!(entries["target"].active_holders.is_empty());
        assert!(entries["target"].last_release_at.is_some());
    }
    let reacquired = manager.acquire("target", "holder-1", unused).unwrap();
    assert_eq!(session.pool_close_count.load(Ordering::SeqCst), 0);
    reacquired.release();

    let manager = Arc::new(TestManager::new("SYSTEM".into()));
    let session = TestSessionManager::new("concurrent");
    let creates = AtomicUsize::new(0);
    let barrier = std::sync::Barrier::new(23);
    let results = std::thread::scope(|scope| {
        let attempts: Vec<_> = (0..23)
            .map(|i| {
                let manager = &manager;
                let session = &session;
                let barrier = &barrier;
                let creates = &creates;
                scope.spawn(move || {
                    let holder = if i < 15 {
                        format!("holderID-{i}")
                    } else {
                        "holder-duplicate".into()
                    };
                    barrier.wait();
                    manager.acquire("concurrent", &holder, |_| {
                        creates.fetch_add(1, Ordering::SeqCst);
                        Ok(session.clone())
                    })
                })
            })
            .collect();
        attempts
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(creates.load(Ordering::SeqCst), 1);
    assert_eq!(manager.get_all_keyspace(), ["concurrent"]);
    assert_eq!(results.iter().filter(|r| r.is_err()).count(), 7);
    for error in results.iter().filter_map(|r| r.as_ref().err()) {
        assert!(error.contains("already acquired"));
    }
    {
        let entries = manager.runtimes.lock().unwrap();
        let holders = &entries["concurrent"].active_holders;
        assert_eq!(holders.len(), 16);
        assert!(holders.contains("holder-duplicate"));
        for i in 0..15 {
            assert!(holders.contains(&format!("holderID-{i}")));
        }
        assert!(Arc::ptr_eq(
            &entries["concurrent"].sess_mgr.sys_session_pool(),
            &session.pool
        ));
    }
    std::thread::scope(|scope| {
        for handle in results.into_iter().filter_map(Result::ok) {
            scope.spawn(move || {
                handle.release();
                handle.release();
            });
        }
    });
    let entries = manager.runtimes.lock().unwrap();
    assert!(entries["concurrent"].active_holders.is_empty());
    assert!(entries["concurrent"].last_release_at.is_some());
    assert_eq!(session.pool_close_count.load(Ordering::SeqCst), 0);
}

/// Go `TestEvictRuntime`, including all three subtests.
#[test]
fn test_evict_runtime() {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    let (manager, session) = new_runtime_handle_test_manager("active");
    let first = manager.acquire("active", "holder-1", unused).unwrap();
    let second = manager.acquire("active", "holder-2", unused).unwrap();
    first.release();
    expire(&manager, "active");
    manager.sweep_idle_runtimes(CROSS_KS_RUNTIME_IDLE_TIMEOUT);
    assert!(manager.get("active").is_some());
    assert!(manager.runtimes.lock().unwrap()["active"]
        .active_holders
        .contains("holder-2"));
    assert_eq!(session.pool_close_count.load(Ordering::SeqCst), 0);
    second.release();

    let (manager, session) = new_runtime_handle_test_manager("idle");
    let weak = Arc::downgrade(&manager);
    *session.on_close.lock().unwrap() = Some(Box::new(move || {
        let mgr = weak.upgrade().unwrap();
        assert!(mgr.runtimes.try_lock().is_ok());
    }));
    let handle = manager.acquire("idle", "holder-1", unused).unwrap();
    handle.release();
    expire(&manager, "idle");
    manager.sweep_idle_runtimes(CROSS_KS_RUNTIME_IDLE_TIMEOUT);
    assert!(manager.get("idle").is_none());
    assert_eq!(session.pool_close_count.load(Ordering::SeqCst), 1);
    assert_eq!(session.store_close_count.load(Ordering::SeqCst), 1);

    let (manager, old) = new_runtime_handle_test_manager("reacquire");
    let handle = manager.acquire("reacquire", "holder-1", unused).unwrap();
    handle.release();
    expire(&manager, "reacquire");
    manager.sweep_idle_runtimes(CROSS_KS_RUNTIME_IDLE_TIMEOUT);
    assert_eq!(old.pool_close_count.load(Ordering::SeqCst), 1);
    assert_eq!(old.store_close_count.load(Ordering::SeqCst), 1);
    let creates = AtomicUsize::new(0);
    let reacquired = manager
        .acquire("reacquire", "holder-2", |ks| {
            creates.fetch_add(1, Ordering::SeqCst);
            Ok(TestSessionManager::new(ks))
        })
        .unwrap();
    assert!(!Arc::ptr_eq(&old.pool, &reacquired.sys_session_pool()));
    assert_eq!(creates.load(Ordering::SeqCst), 1);
    reacquired.release();
}

/// Go `TestRuntimeHandleManagerCloseClosesAllEntriesRegardlessOfIdleTimeout`.
#[test]
fn test_runtime_handle_manager_close_closes_all_entries_regardless_of_idle_timeout() {
    let (manager, first) = new_runtime_handle_test_manager("first");
    let second = TestSessionManager::new("second");
    manager.runtimes.lock().unwrap().insert(
        "second".into(),
        RuntimeEntry {
            sess_mgr: second.clone(),
            active_holders: HashSet::new(),
            last_release_at: None,
        },
    );
    manager
        .runtimes
        .lock()
        .unwrap()
        .get_mut("first")
        .unwrap()
        .last_release_at = Some(Instant::now());
    manager.close();
    assert!(manager.get_all_keyspace().is_empty());
    for session in [first, second] {
        assert_eq!(session.pool_close_count.load(Ordering::SeqCst), 1);
        assert_eq!(session.store_close_count.load(Ordering::SeqCst), 1);
    }
}

/// Go `TestGCLoopExitsWhenContextCancelled`.
#[test]
fn test_gc_loop_exits_when_context_cancelled() {
    let manager = Arc::new(TestManager::new("SYSTEM".into()));
    let (ctx, cancel) = Context::with_cancel(&Context::default());
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        manager.run_system_ks_gc_loop(&ctx);
        tx.send(()).unwrap();
    });
    cancel.cancel();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    worker.join().unwrap();
}

/// Go `TestManagerInClassical` and `TestManager/same keyspace access`.
#[test]
fn test_manager_in_classical() {
    let manager = TestManager::new("SYSTEM".into());
    if tidb_config_kerneltype::is_classic() {
        assert!(manager
            .get_or_create("aaa", unused)
            .err()
            .unwrap()
            .contains("classic kernel or current keyspace"));
    }
    assert!(manager
        .get_or_create("SYSTEM", unused)
        .err()
        .unwrap()
        .contains("classic kernel or current keyspace"));
}

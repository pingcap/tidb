//! Go `pkg/table/tables/state_remote_test.go` and the cached table's runtime,
//! over the node's own internal sessions.

use std::sync::Arc;
use std::time::Duration;

use tidb_table::tables::state_remote::{CachedTableLockType, StateRemote, StateRemoteHandle};

use super::node_fixture::{rows, session_context};
use crate::domain::cluster_session_node::DomainSessions as _;
use super::unistore_cop::cop_backed_stack;
use crate::domain::cluster_session_node::sessions::cluster_stats_session;

/// Go `NewStateRemote(se)` over one of the domain's internal sessions, which
/// the handle keeps (Go's cached table never returns its session).
fn state_remote(domain: &Arc<tidb_domain_core::domain::Domain>) -> StateRemoteHandle {
    let lease = domain
        .advanced_sys_session_pool()
        .get()
        .expect("an internal session");
    let exec = lease
        .with_session_context(|context| {
            Ok::<_, tidb_syssession::SysSessionError>(tidb_syssession::SessionContext::sql_executor(
                cluster_stats_session(context),
            ))
        })
        .expect("its executor");
    std::mem::forget(lease);
    StateRemoteHandle::new(exec)
}

/// Go `oracle.GoTimeToTS(time.Now())`.
fn now_ts() -> u64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    millis << 18
}

fn ts_after(ts: u64, after: Duration) -> u64 {
    ((ts >> 18) + after.as_millis() as u64) << 18
}

/// Go `TestStateRemote`.
#[test]
fn state_remote_moves_the_lock_meta_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(161))
        .expect("session opens");
    let mut h = state_remote(&stack.factory);

    // Check the initial value (Go `initRow`).
    rows(&mut session, "insert ignore into mysql.table_cache_meta values (5, 'NONE', 0, 0)");
    let (lock_type, lease) = h.load(5).unwrap();
    assert_eq!(lock_type, CachedTableLockType::None);
    assert_eq!(lock_type.to_string(), "NONE");
    assert_eq!(lease, 0);

    let ts = now_ts();
    let mut lease_val = ts_after(ts, Duration::from_millis(200));

    // Check read lock.
    assert!(h.lock_for_read(5, lease_val).unwrap());
    let (lock_type, lease) = h.load(5).unwrap();
    assert_eq!(lock_type, CachedTableLockType::Read);
    assert_eq!(lock_type.to_string(), "READ");
    assert_eq!(lease, lease_val);

    // LockForRead while the read lock is held renews the lease.
    assert!(h.lock_for_read(5, lease_val + 1).unwrap());
    let (lock_type, lease) = h.load(5).unwrap();
    assert_eq!(lock_type, CachedTableLockType::Read);
    assert_eq!(lease, lease_val + 1);

    // Renew the read lock's lease.
    lease_val = ts_after(ts, Duration::from_millis(400));
    lease_val = h.renew_read_lease(5, lease, lease_val).unwrap();
    assert!(lease_val > 0);
    let (lock_type, lease) = h.load(5).unwrap();
    assert_eq!(lock_type, CachedTableLockType::Read);
    assert_eq!(lease, lease_val);

    // Check write lock: READ goes INTEND, and the WRITE waits out the read
    // lease before it is granted.
    let read_lease_left = Duration::from_millis((lease_val >> 18).saturating_sub(now_ts() >> 18));
    let started = std::time::Instant::now();
    let write_lease = h.lock_for_write(5, Duration::from_secs(3)).unwrap();
    assert!(started.elapsed() + Duration::from_millis(20) >= read_lease_left);
    let (lock_type, lease) = h.load(5).unwrap();
    assert_eq!(lock_type, CachedTableLockType::Write);
    assert_eq!(lock_type.to_string(), "WRITE");
    assert_eq!(write_lease, lease);
    assert!(write_lease > lease_val);

    // Lock for write again.
    h.lock_for_write(5, Duration::from_secs(3)).unwrap();
    let (lock_type, lease) = h.load(5).unwrap();
    assert_eq!(lock_type, CachedTableLockType::Write);

    // Renewing the read lease fails while the write lock is held.
    assert_eq!(h.renew_read_lease(5, lease, lease + 1).unwrap(), 0);

    // So does acquiring the read lock.
    assert!(!h.lock_for_read(5, lease + 1).unwrap());

    // Renew the write lease.
    assert!(h.renew_write_lease(5, write_lease + 1).unwrap());
    let (lock_type, lease) = h.load(5).unwrap();
    assert_eq!(lock_type, CachedTableLockType::Write);
    assert_eq!(lease, write_lease + 1);

    // Another node's write intention: a WRITE waits until the READ lease the
    // intention recorded is over.
    let old_read_lease = ts_after(now_ts(), Duration::from_millis(300));
    rows(
        &mut session,
        &format!(
            "update mysql.table_cache_meta set lock_type = 'INTEND', oldReadLease = {old_read_lease}, \
             lease = {old_read_lease} where tid = 5"
        ),
    );
    let mut other = state_remote(&stack.factory);
    let read_lease_left =
        Duration::from_millis((old_read_lease >> 18).saturating_sub(now_ts() >> 18));
    let started = std::time::Instant::now();
    other.lock_for_write(5, Duration::from_secs(3)).unwrap();
    assert!(started.elapsed() + Duration::from_millis(20) >= read_lease_left);
    assert_eq!(other.load(5).unwrap().0, CachedTableLockType::Write);
}

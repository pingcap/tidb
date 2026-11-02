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

/// Go `infoschema.tableFromMeta`: a cached table in the domain's schema image
/// is a `cachedTable` whose lock meta runs on one of the domain's internal
/// sessions; `ALTER TABLE ... NOCACHE` makes it a plain table again.
#[test]
fn the_schema_image_gives_a_cached_table_its_lock_meta_session() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(162))
        .expect("session opens");
    rows(&mut session, "create table test.ct (id int primary key, v int)");
    rows(&mut session, "create table test.plain (id int primary key)");
    rows(&mut session, "alter table test.ct cache");
    let catalog = stack.factory.catalog.get_latest();
    let table_id = |name: &str| catalog.table_info_by_name("test", name).unwrap().1.read().id;
    let plain = catalog.physical_kv_table_by_id(table_id("plain")).unwrap();
    assert!(plain.cached_table().is_none());
    let id = table_id("ct");
    let table = catalog.physical_kv_table_by_id(id).unwrap();
    let cached = Arc::clone(table.cached_table().expect("Go's cachedTable"));

    let lease = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (exit, exit_rx) = std::sync::mpsc::channel();
    let (wg, wg_rx) = std::sync::mpsc::channel();
    let keeper = {
        let lease = Arc::clone(&lease);
        std::thread::spawn(move || cached.write_lock_and_keep_alive(&exit_rx, &lease, &wg))
    };
    wg_rx.recv().unwrap().expect("the write lock");
    let lock = rows(
        &mut session,
        &format!("select cast(lock_type as char), lease from mysql.table_cache_meta where tid = {id}"),
    );
    assert!(matches!(&lock[0][0], tidb_datatype::Datum::String(text) if text.bytes() == b"WRITE"));
    assert_eq!(
        lock[0][1].get_int64() as u64,
        lease.load(std::sync::atomic::Ordering::SeqCst)
    );
    drop(exit);
    keeper.join().unwrap();

    rows(&mut session, "alter table test.ct nocache");
    let catalog = stack.factory.catalog.get_latest();
    assert!(catalog.physical_kv_table_by_id(id).unwrap().cached_table().is_none());
}

fn int_rows(session: &mut crate::Session, sql: &str) -> Vec<Vec<i64>> {
    rows(session, sql)
        .iter()
        .map(|row| row.iter().map(tidb_datatype::Datum::get_int64).collect())
        .collect()
}

/// Runs `sql` until the statement reads the cached table's copy (Go's
/// `lastReadFromCache` loops), checking every answer.
fn read_until_cached(session: &mut crate::Session, sql: &str, expected: &[Vec<i64>]) {
    for _ in 0..200 {
        assert_eq!(int_rows(session, sql), expected, "{sql}");
        if session.read_from_table_cache() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{sql} never read the cache");
}

/// Go `TestCacheTableBasicScan`: the first read of a cached table takes the
/// read lock and loads its copy; later reads are answered from the copy by
/// the table reader, the index lookup and a join with a plain table, with
/// the same rows.
#[test]
fn cached_table_reads_come_from_its_copy_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(163))
        .expect("session opens");
    rows(&mut session, "create table test.tmp1 (id int primary key auto_increment, u int unique, v int)");
    rows(
        &mut session,
        "insert into test.tmp1 values (1, 101, 1001), (3, 113, 1003), (5, 105, 1005), (7, 117, 1007)",
    );
    rows(&mut session, "create table test.plain (id int)");
    rows(&mut session, "insert into test.plain values (3)");
    rows(&mut session, "alter table test.tmp1 cache");

    // The first read reads the table and starts the load.
    let all = vec![vec![1, 101, 1001], vec![3, 113, 1003], vec![5, 105, 1005], vec![7, 117, 1007]];
    assert_eq!(int_rows(&mut session, "select * from test.tmp1 order by id"), all);
    assert!(!session.read_from_table_cache());

    read_until_cached(&mut session, "select * from test.tmp1 where id > 3 order by id", &all[2..]);
    // The read lock the copy is served under.
    let lock = rows(
        &mut session,
        "select cast(lock_type as char) from mysql.table_cache_meta m join information_schema.tables t \
         on m.tid = t.tidb_table_id where t.table_name = 'tmp1'",
    );
    assert!(
        matches!(&lock[0][0], tidb_datatype::Datum::String(text) if text.bytes() == b"READ"),
        "{lock:?}"
    );
    // Go's IndexLookUpReader.
    read_until_cached(
        &mut session,
        "select /*+ use_index(tmp1, u) */ * from test.tmp1 where u > 101 order by u",
        &[vec![5, 105, 1005], vec![3, 113, 1003], vec![7, 117, 1007]],
    );
    read_until_cached(
        &mut session,
        "select t.id, p.id from test.tmp1 t join test.plain p on t.id = p.id",
        &[vec![3, 3]],
    );
    // A plain table never reads a cache.
    assert_eq!(int_rows(&mut session, "select * from test.plain"), [vec![3]]);
    assert!(!session.read_from_table_cache());
    // EXPLAIN neither reads the copy's flag into a later statement.
    rows(&mut session, "explain select * from test.tmp1");
}

/// Go `TestCacheTableBasicReadAndWrite` / `TestCacheTableWriteOperatorWaitLockLease`:
/// a write to a cached table takes the WRITE lock before it commits, which
/// waits out the readers' lease; readers then read the table itself until the
/// write lease is over, and the copy they load afterwards has the write.
#[test]
fn a_cached_table_write_waits_out_the_read_lease_and_is_read_after() {
    let (stack, _users) = cop_backed_stack();
    let mut reader = stack
        .factory
        .open_session(session_context(164))
        .expect("session opens");
    let mut writer = stack
        .factory
        .open_session(session_context(165))
        .expect("session opens");
    rows(&mut reader, "create table test.write_tmp1 (id int primary key auto_increment, u int unique, v int)");
    rows(&mut reader, "insert into test.write_tmp1 values (1, 101, 1001), (3, 113, 1003)");
    rows(&mut reader, "alter table test.write_tmp1 cache");
    let sql = "select * from test.write_tmp1 order by id";
    read_until_cached(&mut reader, sql, &[vec![1, 101, 1001], vec![3, 113, 1003]]);

    rows(&mut writer, "insert into test.write_tmp1 values (2, 222, 222)");
    assert!(!writer.wait_lock_lease_time().is_zero(), "the insert waited for the write lock");
    let written = [vec![1, 101, 1001], vec![2, 222, 222], vec![3, 113, 1003]];
    // The write lock is held: the reader reads the table.
    assert_eq!(int_rows(&mut reader, sql), written);
    assert!(!reader.read_from_table_cache());
    // Once the write lease is over, the reloaded copy has the row.
    read_until_cached(&mut reader, sql, &written);

    rows(&mut writer, "update test.write_tmp1 set v = 3333 where id = 2");
    read_until_cached(
        &mut reader,
        sql,
        &[vec![1, 101, 1001], vec![2, 222, 3333], vec![3, 113, 1003]],
    );
    // A read-only statement never waits for a lock.
    rows(&mut writer, "select * from test.write_tmp1");
    assert!(writer.wait_lock_lease_time().is_zero());
}

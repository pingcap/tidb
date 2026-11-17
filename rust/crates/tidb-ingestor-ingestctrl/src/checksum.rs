// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/checksum.go`: service GC safepoint ownership.
use crate::{Error, Result};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::thread::JoinHandle;
use std::time::Duration;
use tidb_gocontext::Context;

/// Go `MinDistSQLScanConcurrency`.
pub const MIN_DISTSQL_SCAN_CONCURRENCY: usize = 4;

/// Go `DefaultBackoffWeight`: `15 * tikvstore.DefBackOffWeight`.
pub const DEFAULT_BACKOFF_WEIGHT: i64 = 15 * 2;

/// Go `RemoteChecksum`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemoteChecksum {
    /// Go `Schema`.
    pub schema: String,
    /// Go `Table`.
    pub table: String,
    /// Go `Checksum`.
    pub checksum: u64,
    /// Go `TotalKVs`.
    pub total_kvs: u64,
    /// Go `TotalBytes`.
    pub total_bytes: u64,
}
impl RemoteChecksum {
    /// Go `RemoteChecksum.IsEqual`, comparing only checksum/row/byte totals.
    pub fn is_equal(&self, other: &tidb_lightning_verification::KvChecksum) -> bool {
        self.checksum == other.sum()
            && self.total_kvs == other.sum_kvs()
            && self.total_bytes == other.sum_size()
    }
}
/// Go `pd.Client.UpdateServiceGCSafePoint` boundary used by `gcTTLManager`.
pub trait PdSafePointClient: Send + Sync {
    /// Go `pd.Client.UpdateServiceGCSafePoint`.
    fn update_service_gc_safe_point(
        &self,
        ctx: &Context,
        service_id: &str,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64>;
}
impl<S: tikv_client::PdClient> PdSafePointClient for tidb_txnkv::driver::tikv_store::TikvStore<S> {
    /// Go `pd.Client.UpdateServiceGCSafePoint` on the connected cluster.
    fn update_service_gc_safe_point(
        &self,
        ctx: &Context,
        service_id: &str,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        if let Some(error) = ctx.err() {
            return Err(Error::Other(error.to_string()));
        }
        let pd = self.store().pd_client();
        self.block_on(async move {
            pd.update_service_gc_safe_point(service_id, ttl, safe_point)
                .await
        })
        .map_err(|error| Error::Other(error.to_string()))
    }
}
struct SafePoints {
    jobs: BTreeMap<(u64, u64), String>,
    next_id: u64,
}
struct Keeper {
    client: Arc<dyn PdSafePointClient>,
    service_id: String,
    jobs: Mutex<SafePoints>,
    last_sp: AtomicU64,
    ttl: i64,
}
impl Keeper {
    /// Go `gcTTLManager.doUpdateGCTTL`.
    fn update(&self, ctx: &Context, ttl: i64, ts: u64) -> Result<()> {
        if ts > 0 {
            self.last_sp.store(ts, Ordering::SeqCst);
            self.client
                .update_service_gc_safe_point(ctx, &self.service_id, ttl, ts)?;
        }
        Ok(())
    }
}
/// Go `gcTTLManager`; a sorted map replaces container/heap while retaining
/// duplicate table names and the minimum active checksum timestamp.
pub struct GcTtlManager {
    inner: Arc<Keeper>,
    worker: Mutex<Option<(crossbeam_channel::Sender<()>, JoinHandle<()>)>>,
}
impl GcTtlManager {
    /// Go `newGCTTLManager`, default serviceSafePointTTL=600 seconds.
    pub fn new(client: Arc<dyn PdSafePointClient>, prefix: &str) -> Self {
        Self::with_ttl(client, prefix, 600)
    }
    /// Go `newGCTTLManager`, with the package's test-configurable serviceSafePointTTL.
    pub fn with_ttl(client: Arc<dyn PdSafePointClient>, prefix: &str, ttl: i64) -> Self {
        assert!(ttl > 0);
        Self {
            inner: Arc::new(Keeper {
                client,
                service_id: format!("{prefix}-{}", uuid::Uuid::new_v4()),
                jobs: Mutex::new(SafePoints {
                    jobs: BTreeMap::new(),
                    next_id: 0,
                }),
                last_sp: AtomicU64::new(0),
                ttl,
            }),
            worker: Mutex::new(None),
        }
    }
    /// Go `gcTTLManager.serviceID`.
    pub fn service_id(&self) -> &str {
        &self.inner.service_id
    }
    /// Go `gcTTLManager.currentTS`.
    pub fn current_ts(&self) -> u64 {
        self.inner
            .jobs
            .lock()
            .unwrap()
            .jobs
            .first_key_value()
            .map_or(0, |(key, _)| key.0)
    }
    /// Go `gcTTLManager.start`.
    fn start(&self, ctx: &Context) {
        let mut worker = self.worker.lock().unwrap();
        if worker.is_some() {
            return;
        }
        let (close, stop) = crossbeam_channel::bounded(1);
        let inner = self.inner.clone();
        let ctx = ctx.clone();
        let _ = inner.update(&ctx, inner.ttl, self.current_ts());
        let handle = std::thread::spawn(move || {
            let tick = crossbeam_channel::tick(Duration::from_secs_f64(inner.ttl as f64 / 3.0));
            loop {
                crossbeam_channel::select! {
                    recv(ctx.done()) -> _ => return,
                    recv(stop) -> _ => { let _ = inner.update(&ctx,0,inner.last_sp.load(Ordering::SeqCst)); return; }
                    recv(tick) -> _ => {
                        let ts = inner.jobs.lock().unwrap().jobs.first_key_value().map_or(0,|(key,_)| key.0);
                        let _ = inner.update(&ctx,inner.ttl,ts);
                    }
                }
            }
        });
        *worker = Some((close, handle));
    }
    /// Go `gcTTLManager.addOneJob`.
    pub fn add_one_job(&self, ctx: &Context, table: &str, ts: u64) -> Result<()> {
        self.start(ctx);
        let mut jobs = self.inner.jobs.lock().unwrap();
        let old = jobs.jobs.first_key_value().map_or(0, |(key, _)| key.0);
        let id = jobs.next_id;
        jobs.next_id += 1;
        jobs.jobs.insert((ts, id), table.into());
        let new = jobs.jobs.first_key_value().unwrap().0.0;
        if old == 0 || new < old {
            self.inner.update(ctx, self.inner.ttl, new)?;
        }
        Ok(())
    }
    /// Go `gcTTLManager.removeOneJob`.
    pub fn remove_one_job(&self, table: &str) {
        let mut jobs = self.inner.jobs.lock().unwrap();
        let key = jobs
            .jobs
            .iter()
            .find(|(_, name)| name.as_str() == table)
            .map(|(key, _)| *key);
        if let Some(key) = key {
            jobs.jobs.remove(&key);
        }
    }
    /// Go `gcTTLManager.close`, removing the last safepoint even with no active jobs.
    pub fn close(&self) {
        if let Some((close, handle)) = self.worker.lock().unwrap().take() {
            let _ = close.send(());
            let _ = handle.join();
        }
    }
}
impl Drop for GcTtlManager {
    /// Go `TiKVChecksumManager.Close` on owner destruction.
    fn drop(&mut self) {
        self.close();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Default)]
    struct Pd {
        points: Mutex<BTreeMap<String, u64>>,
        calls: AtomicU64,
    }
    impl PdSafePointClient for Pd {
        /// Go `pd.Client.UpdateServiceGCSafePoint`.
        fn update_service_gc_safe_point(
            &self,
            _: &Context,
            id: &str,
            ttl: i64,
            ts: u64,
        ) -> Result<u64> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut points = self.points.lock().unwrap();
            if ttl == 0 {
                points.remove(id);
            } else {
                points.insert(id.into(), ts);
            }
            Ok(points.values().copied().min().unwrap_or(0))
        }
    }
    /// Go `TestGcTTLManagerSingle`.
    #[test]
    fn test_gc_ttl_manager_single() {
        let pd = Arc::new(Pd::default());
        let manager = GcTtlManager::with_ttl(pd.clone(), "lightning", 1);
        manager
            .add_one_job(&Context::background(), "test", 123)
            .unwrap();
        std::thread::sleep(Duration::from_millis(2010));
        assert!(pd.calls.load(Ordering::SeqCst) >= 5);
        manager.remove_one_job("test");
        assert!(pd.points.lock().unwrap().contains_key(manager.service_id()));
        manager.close();
        assert!(!pd.points.lock().unwrap().contains_key(manager.service_id()));
    }
    /// Go `TestGcTTLManagerMulti`.
    #[test]
    fn test_gc_ttl_manager_multi() {
        let pd = Arc::new(Pd::default());
        let manager = GcTtlManager::new(pd.clone(), "lightning");
        for i in 1..=5 {
            manager
                .add_one_job(&Context::background(), &format!("test{i}"), i)
                .unwrap();
            assert_eq!(manager.current_ts(), 1);
        }
        for (i, expected) in [(2, 1), (1, 3), (3, 4), (4, 5), (5, 0)] {
            manager.remove_one_job(&format!("test{i}"));
            assert_eq!(manager.current_ts(), expected);
        }
        assert!(pd.points.lock().unwrap().contains_key(manager.service_id()));
        manager.close();
        assert!(!pd.points.lock().unwrap().contains_key(manager.service_id()));
    }
    /// Go `TestPdServiceID`.
    #[test]
    fn test_pd_service_id() {
        let pd = Arc::new(Pd::default());
        let a = GcTtlManager::new(pd.clone(), "lightning");
        let b = GcTtlManager::new(pd, "lightning");
        assert!(a.service_id().starts_with("lightning-"));
        assert!(b.service_id().starts_with("lightning-"));
        assert_ne!(a.service_id(), b.service_id());
    }
}

/// Go `database/sql.Conn` operations used by `tidbChecksumExecutor`.
/// A connection must retain session state between these calls.
pub trait SqlConn: Send {
    /// Go `Conn.QueryRowContext(...).Scan(...)`; numeric fields use decimal text.
    fn query_row(&mut self, ctx: &Context, sql: &str) -> Result<Vec<String>>;
    /// Go `Conn.ExecContext` with bound string parameters.
    fn exec(&mut self, ctx: &Context, sql: &str, arguments: &[&str]) -> Result<()>;
}
/// Go `database/sql.DB`; the pool and borrowed connection have separate lifetimes.
pub trait SqlDb: Send + Sync {
    /// Go `DB.Conn`.
    fn conn(&self, ctx: &Context) -> Result<Box<dyn SqlConn>>;
}
const GC_QUERY: &str =
    "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'";
const GC_UPDATE: &str =
    "UPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'";
/// Go `common.SQLWithRetry.perform`, three attempts with three-second delay.
fn sql_retry<T>(purpose: &str, mut operation: impl FnMut() -> Result<T>) -> Result<T> {
    for attempt in 0..3 {
        if attempt != 0 {
            std::thread::sleep(Duration::from_secs(3));
        }
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if attempt < 2 && tidb_lightning_common::is_retryable_error(&error) => {}
            Err(error) => return Err(Error::Other(format!("{purpose} failed: {error}"))),
        }
    }
    unreachable!()
}
/// Go `obtainGCLifeTime`.
pub fn obtain_gc_life_time(ctx: &Context, db: &dyn SqlDb) -> Result<String> {
    sql_retry("obtain GC lifetime", || {
        db.conn(ctx)?
            .query_row(ctx, GC_QUERY)?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Other("sql: no rows in result set".into()))
    })
}
/// Go `updateGCLifeTime`.
pub fn update_gc_life_time(ctx: &Context, db: &dyn SqlDb, value: &str) -> Result<()> {
    sql_retry("update GC lifetime", || {
        db.conn(ctx)?.exec(ctx, GC_UPDATE, &[value])
    })
}
#[derive(Default)]
struct LifeTime {
    running_jobs: usize,
    original: String,
}
/// Go `gcLifeTimeManager`.
#[derive(Default)]
pub struct GcLifeTimeManager {
    state: Mutex<LifeTime>,
}
impl GcLifeTimeManager {
    /// Go `gcLifeTimeManager.addOneJob`; a failed increase never acquires a job.
    pub fn add_one_job(&self, ctx: &Context, db: &dyn SqlDb) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.running_jobs == 0 {
            state.original = obtain_gc_life_time(ctx, db)?;
            let duration = if state.original.is_empty() {
                0
            } else {
                tidb_config_configtypes::parse_duration(&state.original).map_err(Error::Other)?
            };
            if duration < 100 * 3600 * 1_000_000_000_i64 {
                update_gc_life_time(ctx, db, "100h0m0s")?;
            }
            tidb_failpoint::inject(
                "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/IncreaseGCUpdateDuration",
                |_| {},
            );
        }
        state.running_jobs += 1;
        Ok(())
    }
    /// Go `gcLifeTimeManager.removeOneJob`; recovery is best effort.
    pub fn remove_one_job(&self, ctx: &Context, db: &dyn SqlDb) {
        let mut state = self.state.lock().unwrap();
        assert!(state.running_jobs > 0);
        state.running_jobs -= 1;
        if state.running_jobs == 0 {
            if let Err(error) = update_gc_life_time(ctx, db, &state.original) {
                tidb_log::warn(
                    "revert GC lifetime failed, please reset the GC lifetime manually after Lightning completed",
                    &[tidb_log::Field::new(
                        "error",
                        tidb_log::Value::Str(error.to_string()),
                    )],
                );
            }
        }
    }
}
/// Go `tidbChecksumExecutor`.
pub struct TiDBChecksumExecutor {
    db: Arc<dyn SqlDb>,
    manager: GcLifeTimeManager,
}
impl TiDBChecksumExecutor {
    /// Go `NewTiDBChecksumExecutor`.
    pub fn new(db: Arc<dyn SqlDb>) -> Self {
        Self {
            db,
            manager: GcLifeTimeManager::default(),
        }
    }
    /// Go `tidbChecksumExecutor.Checksum`; guards restore GC and session backoff
    /// on both successful checksum and query failure.
    fn checksum_table(&self, ctx: &Context, schema: &str, table: &str) -> Result<RemoteChecksum> {
        self.manager.add_one_job(ctx, self.db.as_ref())?;
        struct Job<'a>(&'a TiDBChecksumExecutor, &'a Context);
        impl Drop for Job<'_> {
            /// Go deferred `gcLifeTimeManager.removeOneJob`.
            fn drop(&mut self) {
                self.0.manager.remove_one_job(self.1, self.0.db.as_ref());
            }
        }
        let _job = Job(self, ctx);
        let mut conn = self.db.conn(ctx)?;
        let weight = self
            .db
            .conn(ctx)
            .and_then(|mut db| db.query_row(ctx, "SHOW VARIABLES LIKE 'tidb_backoff_weight'"))
            .ok()
            .and_then(|row| row.get(1)?.parse::<i64>().ok());
        let restore = weight.filter(|weight| *weight < 30).filter(|_| {
            conn.exec(ctx, "SET SESSION tidb_backoff_weight = '30';", &[])
                .is_ok()
        });
        let sql = format!(
            "ADMIN CHECKSUM TABLE `{}`.`{}`",
            schema.replace('`', "``"),
            table.replace('`', "``")
        );
        let result =
            sql_retry("compute remote checksum", || conn.query_row(ctx, &sql)).and_then(|row| {
                if row.len() != 5 {
                    return Err(Error::Other("invalid ADMIN CHECKSUM result".into()));
                }
                let number = |index: usize| {
                    row[index]
                        .parse::<u64>()
                        .map_err(|e| Error::Other(e.to_string()))
                };
                Ok(RemoteChecksum {
                    schema: row[0].clone(),
                    table: row[1].clone(),
                    checksum: number(2)?,
                    total_kvs: number(3)?,
                    total_bytes: number(4)?,
                })
            });
        if let Some(weight) = restore {
            let _ = conn.exec(
                ctx,
                &format!("SET SESSION tidb_backoff_weight = '{weight}';"),
                &[],
            );
        }
        result
    }
    /// Go `tidbChecksumExecutor.Close` does not own the SQL pool.
    pub fn close(&self) {}
}

#[cfg(test)]
mod sql_tests {
    use super::*;
    struct State {
        lifetime: String,
        updates: Vec<String>,
        fail_update: bool,
        fail_checksum: bool,
        barrier: Option<Arc<std::sync::Barrier>>,
    }
    #[derive(Clone)]
    struct Db(Arc<Mutex<State>>);
    impl Db {
        /// Go `TestDoChecksum SQL mock`.
        fn new(lifetime: &str) -> Self {
            Self(Arc::new(Mutex::new(State {
                lifetime: lifetime.into(),
                updates: vec![],
                fail_update: false,
                fail_checksum: false,
                barrier: None,
            })))
        }
    }
    impl SqlDb for Db {
        /// Go `sql.DB.Conn`.
        fn conn(&self, _: &Context) -> Result<Box<dyn SqlConn>> {
            Ok(Box::new(self.clone()))
        }
    }
    impl SqlConn for Db {
        /// Go `sql.Conn.QueryRowContext`.
        fn query_row(&mut self, _: &Context, sql: &str) -> Result<Vec<String>> {
            let state = self.0.lock().unwrap();
            if sql == GC_QUERY {
                return Ok(vec![state.lifetime.clone()]);
            }
            if sql == "SHOW VARIABLES LIKE 'tidb_backoff_weight'" {
                return Err(Error::Other("unsupported session variable".into()));
            }
            assert_eq!(sql, "ADMIN CHECKSUM TABLE `test`.`t`");
            if state.fail_checksum {
                return Err(Error::Other("mock syntax error: context canceled".into()));
            }
            let barrier = state.barrier.clone();
            drop(state);
            if let Some(barrier) = barrier {
                barrier.wait();
            }
            Ok(["test", "t", "8520875019404689597", "7296873", "357601387"]
                .map(String::from)
                .into())
        }
        /// Go `sql.Conn.ExecContext`.
        fn exec(&mut self, _: &Context, sql: &str, args: &[&str]) -> Result<()> {
            assert_eq!(sql, GC_UPDATE);
            let mut state = self.0.lock().unwrap();
            state.updates.push(args[0].into());
            if state.fail_update {
                return Err(Error::Other("update gc error: context canceled".into()));
            }
            state.lifetime = args[0].into();
            Ok(())
        }
    }
    /// Go `TestDoChecksum expected result`.
    fn expected() -> RemoteChecksum {
        RemoteChecksum {
            schema: "test".into(),
            table: "t".into(),
            checksum: 8520875019404689597,
            total_kvs: 7296873,
            total_bytes: 357601387,
        }
    }
    /// Go `TestDoChecksum`.
    #[test]
    fn test_do_checksum() {
        let db = Arc::new(Db::new("10m"));
        assert_eq!(
            TiDBChecksumExecutor::new(db.clone())
                .checksum_table(&Context::background(), "test", "t")
                .unwrap(),
            expected()
        );
        assert_eq!(db.0.lock().unwrap().updates, ["100h0m0s", "10m"]);
        let checksum = expected();
        let local = tidb_lightning_verification::KvChecksum::make(
            checksum.total_bytes,
            checksum.total_kvs,
            checksum.checksum,
        );
        assert!(checksum.is_equal(&local));
        for mismatch in [
            (
                checksum.total_bytes + 1,
                checksum.total_kvs,
                checksum.checksum,
            ),
            (
                checksum.total_bytes,
                checksum.total_kvs + 1,
                checksum.checksum,
            ),
            (
                checksum.total_bytes,
                checksum.total_kvs,
                checksum.checksum + 1,
            ),
        ] {
            assert!(
                !checksum.is_equal(&tidb_lightning_verification::KvChecksum::make(
                    mismatch.0, mismatch.1, mismatch.2
                ))
            );
        }
    }
    /// Go `TestDoChecksumParallel`.
    #[test]
    fn test_do_checksum_parallel() {
        let db = Arc::new(Db::new("10m"));
        db.0.lock().unwrap().barrier = Some(Arc::new(std::sync::Barrier::new(5)));
        let executor = TiDBChecksumExecutor::new(db.clone());
        std::thread::scope(|scope| {
            for _ in 0..5 {
                let executor = &executor;
                scope.spawn(move || {
                    assert_eq!(
                        executor
                            .checksum_table(&Context::background(), "test", "t")
                            .unwrap(),
                        expected()
                    )
                });
            }
        });
        assert_eq!(db.0.lock().unwrap().updates, ["100h0m0s", "10m"]);
    }
    /// Go `TestIncreaseGCLifeTimeFail`.
    #[test]
    fn test_increase_gc_life_time_fail() {
        let db = Arc::new(Db::new("10m"));
        db.0.lock().unwrap().fail_update = true;
        let executor = TiDBChecksumExecutor::new(db.clone());
        std::thread::scope(|scope| {
            for _ in 0..5 {
                let executor = &executor;
                scope.spawn(move || {
                    assert_eq!(
                        executor
                            .checksum_table(&Context::background(), "test", "t")
                            .unwrap_err()
                            .to_string(),
                        "update GC lifetime failed: update gc error: context canceled"
                    )
                });
            }
        });
        assert_eq!(db.0.lock().unwrap().updates, vec!["100h0m0s"; 5]);
        assert_eq!(executor.manager.state.lock().unwrap().running_jobs, 0);
    }
    /// Go `TestDoChecksumWithErrorAndLongOriginalLifetime`.
    #[test]
    fn test_do_checksum_with_error_and_long_original_lifetime() {
        let db = Arc::new(Db::new("300h"));
        db.0.lock().unwrap().fail_checksum = true;
        assert_eq!(
            TiDBChecksumExecutor::new(db.clone())
                .checksum_table(&Context::background(), "test", "t")
                .unwrap_err()
                .to_string(),
            "compute remote checksum failed: mock syntax error: context canceled"
        );
        assert_eq!(db.0.lock().unwrap().updates, ["300h"]);
    }
    /// Go `TestGetGCLifetime`.
    #[test]
    fn test_get_gc_lifetime() {
        assert_eq!(
            obtain_gc_life_time(&Context::background(), &Db::new("10m")).unwrap(),
            "10m"
        );
    }
    /// Go `TestSetGCLifetime`.
    #[test]
    fn test_set_gc_lifetime() {
        let db = Db::new("10m");
        update_gc_life_time(&Context::background(), &db, "12m").unwrap();
        assert_eq!(db.0.lock().unwrap().lifetime, "12m");
    }
}

/// Go `pd.Client` operations used by `TiKVChecksumManager`.
pub trait ChecksumPdClient: PdSafePointClient {
    /// Go `pd.Client.GetTS`, as the composed physical/logical timestamp.
    fn get_ts(&self, ctx: &Context) -> Result<u64>;
}
impl<S: tikv_client::PdClient> ChecksumPdClient for tidb_txnkv::driver::tikv_store::TikvStore<S> {
    /// Go `pd.Client.GetTS` and `oracle.ComposeTS`.
    fn get_ts(&self, ctx: &Context) -> Result<u64> {
        use tikv_client::TimestampExt;
        if let Some(error) = ctx.err() {
            return Err(Error::Source(Arc::new(error)));
        }
        let pd = self.store().pd_client();
        self.block_on(async move { pd.get_timestamp().await })
            .map(|ts| ts.version())
            .map_err(|error| Error::Source(Arc::new(error)))
    }
}
/// Go `TiKVChecksumManager`.
pub struct TiKVChecksumManager {
    client: Arc<dyn tidb_txnkv::Client>,
    pd: Arc<dyn ChecksumPdClient>,
    manager: GcTtlManager,
    concurrency: u64,
    backoff_weight: i32,
    resource_group_name: String,
    request_source: tidb_txnkv::RequestSource,
    retry_get_ts_interval: Duration,
    checksum_retry_interval: Duration,
}
impl TiKVChecksumManager {
    /// Go `NewTiKVChecksumManager`.
    pub fn new(
        client: Arc<dyn tidb_txnkv::Client>,
        pd: Arc<dyn ChecksumPdClient>,
        concurrency: u64,
        backoff_weight: i32,
        resource_group_name: String,
        explicit_request_source: String,
    ) -> Self {
        Self {
            client,
            manager: GcTtlManager::new(pd.clone(), "lightning"),
            pd,
            concurrency,
            backoff_weight,
            resource_group_name,
            request_source: tidb_txnkv::RequestSource {
                explicit_source_type: explicit_request_source,
                ..Default::default()
            },
            retry_get_ts_interval: Duration::from_secs(1),
            checksum_retry_interval: Duration::from_secs(1),
        }
    }
    /// Go `NewTiKVChecksumManagerForImportInto`.
    pub fn for_import_into(
        client: Arc<dyn tidb_txnkv::Client>,
        pd: Arc<dyn ChecksumPdClient>,
        task_id: i64,
        concurrency: u64,
        backoff_weight: i32,
        resource_group_name: String,
    ) -> Self {
        let mut manager = Self::new(
            client,
            pd.clone(),
            concurrency,
            backoff_weight,
            resource_group_name,
            String::new(),
        );
        manager.manager = GcTtlManager::new(pd, &format!("import-into-{task_id}"));
        manager.request_source = tidb_txnkv::RequestSource {
            internal: true,
            source_type: "import_into".into(),
            ..Default::default()
        };
        manager
    }
    /// Go `TiKVChecksumManager.Checksum`.
    fn checksum_table(
        &self,
        ctx: &Context,
        schema: &str,
        table: &str,
        info: &tidb_model::TableInfo,
    ) -> Result<RemoteChecksum> {
        let ts = loop {
            match self.pd.get_ts(ctx) {
                Ok(ts) => break ts,
                Err(error) => {
                    let cause: &(dyn std::error::Error + 'static) = match &error {
                        Error::Source(cause) => cause.as_ref(),
                        _ => &error,
                    };
                    if !tikv_client::pd_errs::is_leader_change(Some(cause)) {
                        return Err(Error::Other(format!("fetch tso from pd failed: {error}")));
                    }
                    if ctx.sleep(self.retry_get_ts_interval) {
                        return Err(Error::Source(Arc::new(ctx.err().unwrap())));
                    }
                }
            }
        };
        let name = format!(
            "`{}`.`{}`",
            schema.replace('`', "``"),
            table.replace('`', "``")
        );
        self.manager.add_one_job(ctx, &name, ts)?;
        struct Job<'a>(&'a GcTtlManager, String);
        impl Drop for Job<'_> {
            /// Go `TiKVChecksumManager.Checksum deferred removeOneJob`.
            fn drop(&mut self) {
                self.0.remove_one_job(&self.1);
            }
        }
        let _job = Job(&self.manager, name);
        self.checksum_db(ctx, schema, table, info, ts)
    }
    /// Go `TiKVChecksumManager.checksumDB`, reducing concurrency only between
    /// complete executor retries, never combining partial results from attempts.
    fn checksum_db(
        &self,
        ctx: &Context,
        schema: &str,
        table: &str,
        info: &tidb_model::TableInfo,
        ts: u64,
    ) -> Result<RemoteChecksum> {
        let mut concurrency = self.concurrency;
        let mut last = None;
        for _ in 0..3 {
            let requests = self.build_checksum_requests(info, ts, concurrency)?;
            match self.execute(ctx, requests) {
                Ok(mut checksum) => {
                    checksum.schema = schema.into();
                    checksum.table = table.into();
                    return Ok(checksum);
                }
                Err(error) => {
                    let retry = tidb_lightning_common::is_retryable_error(&error);
                    last = Some(error);
                    if !retry {
                        break;
                    }
                    if concurrency > 4 {
                        concurrency = (concurrency / 2).max(4);
                    }
                }
            }
        }
        Err(last.unwrap())
    }
    /// Go `checksum.ExecutorBuilder.Build` for ingestion's no-rewrite request.
    /// The parent checksum package's backup/restore rewrite rules are not used
    /// by ingestctrl; all logical/partition record and public index ranges are.
    fn build_checksum_requests(
        &self,
        table: &tidb_model::TableInfo,
        ts: u64,
        concurrency: u64,
    ) -> Result<Vec<tidb_txnkv::KvRequestMetadata>> {
        use prost::Message;
        use tidb_proto::tipb;
        let mut requests = Vec::new();
        for id in crate::duplicate::physical_table_ids(table) {
            let mut indexes = vec![None];
            for index in table.indices.iter_deref() {
                if index.read().state == tidb_model::SchemaState::PUBLIC {
                    indexes.push(Some(index.read().id));
                }
            }
            for index in indexes {
                let mut builder = tidb_distsql::RequestBuilder::new();
                let scan_on = if let Some(index) = index {
                    builder.set_index_ranges(None, id, index, &tidb_ranger::points::full_range());
                    tipb::ChecksumScanOn::Index
                } else {
                    let ranges = if table.is_common_handle {
                        tidb_ranger::points::full_not_null_range()
                    } else {
                        tidb_ranger::points::full_int_range(false)
                    };
                    builder.set_handle_ranges(None, id, table.is_common_handle, &ranges);
                    tipb::ChecksumScanOn::Table
                };
                let checksum = tipb::ChecksumRequest {
                    scan_on: Some(scan_on as i32),
                    algorithm: Some(tipb::ChecksumAlgorithm::Crc64Xor as i32),
                    ..Default::default()
                };
                builder
                    .set_checksum_request(checksum.encode_to_vec())
                    .set_start_ts(ts)
                    .set_concurrency(concurrency)
                    .set_resource_group_name(self.resource_group_name.clone())
                    .set_request_source(self.request_source.clone())
                    .set_paging(false);
                let mut request = builder
                    .build()
                    .map_err(|error| Error::Other(format!("{error:?}")))?;
                request.priority = tidb_txnkv::Priority::Low;
                requests.push(request);
            }
        }
        Ok(requests)
    }
    /// Go `checksum.Executor.Execute` / `sendChecksumRequest`: consume and
    /// close each raw coprocessor response, with BR's eight-attempt backoff.
    fn execute(
        &self,
        ctx: &Context,
        requests: Vec<tidb_txnkv::KvRequestMetadata>,
    ) -> Result<RemoteChecksum> {
        use prost::Message;
        let cancellation = Arc::new(tidb_txnkv::CancelHandle::default());
        let (stop, stopped) = crossbeam_channel::bounded::<()>(0);
        std::thread::scope(|scope| {
            let cancellation = &cancellation;
            scope.spawn(|| { crossbeam_channel::select! { recv(ctx.done()) -> _ => cancellation.cancel(),recv(stopped) -> _ => {} } });
            let result = (|| {
                let mut total = RemoteChecksum::default();
                let mut variables = tikv_client::Variables::default();
                if self.backoff_weight > 0 {
                    variables.backoff_weight = self.backoff_weight;
                }
                let variables = Arc::new(variables);
                for request in requests {
                    let mut response = None;
                    for attempt in 0..8 {
                        if let Some(error) = ctx.err() {
                            return Err(Error::Source(Arc::new(error)));
                        }
                        let request = tidb_txnkv::TransportRequest::new(
                            request.clone(),
                            cancellation.clone(),
                        )
                        .with_variables(variables.clone());
                        let result = (|| {
                            let mut raw = tidb_distsql::checksum(self.client.as_ref(), &request)
                                .map_err(Error::Other)?;
                            let read = (|| {
                                let mut checksum = RemoteChecksum::default();
                                while let Some(bytes) =
                                    raw.next_raw().map_err(checksum_response_error)?
                                {
                                    let part = tidb_proto::tipb::ChecksumResponse::decode(
                                        bytes.as_slice(),
                                    )
                                    .map_err(|error| Error::Source(Arc::new(error)))?;
                                    checksum.checksum ^= part.checksum();
                                    checksum.total_kvs =
                                        checksum.total_kvs.wrapping_add(part.total_kvs());
                                    checksum.total_bytes =
                                        checksum.total_bytes.wrapping_add(part.total_bytes());
                                }
                                Ok(checksum)
                            })();
                            let closed = raw.close().map_err(checksum_response_error);
                            match closed {
                                Err(error) => Err(error),
                                Ok(()) => read,
                            }
                        })();
                        match result {
                            Ok(checksum) => {
                                response = Some(checksum);
                                break;
                            }
                            Err(error) => {
                                if attempt == 7
                                    || ctx.err().is_some()
                                    || matches!(&error,Error::Source(source) if source.downcast_ref::<tidb_gocontext::CtxError>() == Some(&tidb_gocontext::CtxError::Canceled))
                                {
                                    return Err(error);
                                }
                                if ctx.sleep(
                                    (self.checksum_retry_interval * (1 << (attempt + 1)))
                                        .min(Duration::from_secs(10)),
                                ) {
                                    return Err(Error::Source(Arc::new(ctx.err().unwrap())));
                                }
                            }
                        }
                    }
                    let response = response.unwrap();
                    total.checksum ^= response.checksum;
                    total.total_kvs = total.total_kvs.wrapping_add(response.total_kvs);
                    total.total_bytes = total.total_bytes.wrapping_add(response.total_bytes);
                }
                if let Some(error) = ctx.err() {
                    return Err(Error::Source(Arc::new(error)));
                }
                Ok(total)
            })();
            drop(stop);
            result
        })
    }
    /// Go `TiKVChecksumManager.Close`.
    pub fn close(&self) {
        self.manager.close();
    }
}
/// Go `ToTiDBErr` identity retained across the raw response boundary.
fn checksum_response_error(error: tidb_txnkv::QueryResponseError) -> Error {
    match error {
        tidb_txnkv::QueryResponseError::Sql { code, message } => {
            Error::Source(Arc::new(tidb_terror::MysqlError::sql_error(code, message)))
        }
        tidb_txnkv::QueryResponseError::Cancelled => {
            Error::Source(Arc::new(tidb_gocontext::CtxError::Canceled))
        }
        error => Error::Source(Arc::new(error)),
    }
}

#[cfg(test)]
mod tikv_tests {
    use super::*;
    use prost::Message;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    #[derive(Default)]
    struct Pd {
        point: AtomicU64,
        present: AtomicBool,
        leader_changing: AtomicBool,
    }
    impl PdSafePointClient for Pd {
        /// Go `pd.Client.UpdateServiceGCSafePoint`.
        fn update_service_gc_safe_point(
            &self,
            _: &Context,
            _: &str,
            ttl: i64,
            ts: u64,
        ) -> Result<u64> {
            self.point.store(ts, Ordering::SeqCst);
            self.present.store(ttl > 0, Ordering::SeqCst);
            Ok(ts)
        }
    }
    impl ChecksumPdClient for Pd {
        /// Go `pd.Client.GetTS`.
        fn get_ts(&self, _: &Context) -> Result<u64> {
            if self.leader_changing.swap(false, Ordering::SeqCst) {
                return Err(Error::Other("not leader".into()));
            }
            Ok(123 << 18)
        }
    }
    #[derive(Debug)]
    struct Client {
        failures: AtomicUsize,
        requests: Mutex<Vec<(u64, isize, i32)>>,
        closes: Arc<AtomicUsize>,
    }
    struct Response {
        error: bool,
        read: bool,
        closes: Arc<AtomicUsize>,
    }
    impl tidb_txnkv::Response for Response {
        /// Go `kv.Response.Next`.
        fn next(
            &mut self,
        ) -> std::result::Result<
            Option<tidb_txnkv::QueryResultSubset>,
            tidb_txnkv::QueryResponseError,
        > {
            if self.error {
                return Err(tidb_txnkv::QueryResponseError::Sql {
                    code: 9001,
                    message: "PD server timeout".into(),
                });
            }
            if self.read {
                return Ok(None);
            }
            self.read = true;
            Ok(Some(tidb_txnkv::QueryResultSubset {
                data: tidb_proto::tipb::ChecksumResponse {
                    checksum: Some(123),
                    total_kvs: Some(10),
                    total_bytes: Some(1000),
                }
                .encode_to_vec()
                .into(),
                ..Default::default()
            }))
        }
        /// Go `kv.Response.Close`.
        fn close(&mut self) -> std::result::Result<(), tidb_txnkv::QueryResponseError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    impl tidb_txnkv::Client for Client {
        /// Go `kv.Client.Send`.
        fn send(
            &self,
            request: &tidb_txnkv::TransportRequest,
        ) -> Box<dyn tidb_txnkv::Response + Send> {
            assert_eq!(request.metadata().priority, tidb_txnkv::Priority::Low);
            self.requests.lock().unwrap().push((
                request.metadata().start_ts,
                request.metadata().concurrency,
                request.variables().unwrap().backoff_weight,
            ));
            let error = self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            Box::new(Response {
                error,
                read: false,
                closes: self.closes.clone(),
            })
        }
        /// Go `kv.Client.IsRequestTypeSupported`.
        fn is_request_type_supported(&self, _: i64, _: i64) -> bool {
            true
        }
    }
    /// Go `TestDoChecksumWithTikv`: retry errors, preserve the read timestamp
    /// in PD, release active jobs, and retry a PD leader change.
    #[test]
    fn test_do_checksum_with_tikv() {
        for errors in 0..=3 {
            let pd = Arc::new(Pd::default());
            let closes = Arc::new(AtomicUsize::new(0));
            let client = Arc::new(Client {
                failures: AtomicUsize::new(errors),
                requests: Mutex::new(vec![]),
                closes: closes.clone(),
            });
            let mut manager = TiKVChecksumManager::new(
                client.clone(),
                pd.clone(),
                16,
                30,
                "rg".into(),
                String::new(),
            );
            manager.checksum_retry_interval = Duration::from_millis(1);
            let checksum = manager
                .checksum_table(
                    &Context::background(),
                    "test",
                    "t",
                    &tidb_model::TableInfo {
                        id: 999,
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(
                checksum,
                RemoteChecksum {
                    schema: "test".into(),
                    table: "t".into(),
                    checksum: 123,
                    total_kvs: 10,
                    total_bytes: 1000
                }
            );
            assert!(pd.present.load(Ordering::SeqCst));
            assert_eq!(manager.manager.current_ts(), 0);
            let requests = client.requests.lock().unwrap();
            assert_eq!(requests.len(), errors + 1);
            assert_eq!(closes.load(Ordering::SeqCst), requests.len());
            assert!(
                requests
                    .iter()
                    .all(|request| request.0 == pd.point.load(Ordering::SeqCst) && request.2 == 30)
            );
            drop(requests);
            manager.close();
            assert!(!pd.present.load(Ordering::SeqCst));
        }
        let pd = Arc::new(Pd::default());
        pd.leader_changing.store(true, Ordering::SeqCst);
        let client = Arc::new(Client {
            failures: AtomicUsize::new(16),
            requests: Mutex::new(vec![]),
            closes: Default::default(),
        });
        let mut manager = TiKVChecksumManager::new(
            client.clone(),
            pd.clone(),
            16,
            30,
            String::new(),
            String::new(),
        );
        manager.retry_get_ts_interval = Duration::from_millis(1);
        manager.checksum_retry_interval = Duration::from_millis(1);
        manager
            .checksum_table(
                &Context::background(),
                "test",
                "t",
                &tidb_model::TableInfo {
                    id: 999,
                    ..Default::default()
                },
            )
            .unwrap();
        let requests = client.requests.lock().unwrap();
        assert_eq!(requests.len(), 17);
        assert_eq!(requests[0].1, 16);
        assert_eq!(requests[8].1, 8);
        assert_eq!(requests[16].1, 4);
        drop(requests);
        manager.close();
        assert!(!pd.present.load(Ordering::SeqCst));
    }
}

/// Go `ChecksumManager`.
pub trait ChecksumManager: Send + Sync {
    /// Go `ChecksumManager.Checksum`.
    fn checksum(
        &self,
        ctx: &Context,
        table: &tidb_lightning_importdef::TableInfo,
    ) -> Result<RemoteChecksum>;
    /// Go `ChecksumManager.Close`.
    fn close(&self);
}
impl ChecksumManager for TiDBChecksumExecutor {
    /// Go `tidbChecksumExecutor.Checksum`.
    fn checksum(
        &self,
        ctx: &Context,
        table: &tidb_lightning_importdef::TableInfo,
    ) -> Result<RemoteChecksum> {
        self.checksum_table(
            ctx,
            &table.db.to_utf8_lossy_go(),
            &table.name.to_utf8_lossy_go(),
        )
    }
    /// Go `tidbChecksumExecutor.Close`.
    fn close(&self) {
        TiDBChecksumExecutor::close(self);
    }
}
impl ChecksumManager for TiKVChecksumManager {
    /// Go `TiKVChecksumManager.Checksum`.
    fn checksum(
        &self,
        ctx: &Context,
        table: &tidb_lightning_importdef::TableInfo,
    ) -> Result<RemoteChecksum> {
        let core = table
            .core
            .as_ref()
            .ok_or_else(|| Error::Other("checksum requires table metadata".into()))?
            .read()
            .unwrap();
        self.checksum_table(
            ctx,
            &table.db.to_utf8_lossy_go(),
            &table.name.to_utf8_lossy_go(),
            &core,
        )
    }
    /// Go `TiKVChecksumManager.Close`.
    fn close(&self) {
        TiKVChecksumManager::close(self);
    }
}

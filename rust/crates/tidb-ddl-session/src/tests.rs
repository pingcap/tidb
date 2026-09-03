// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::ScheduleEvalOriginals;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_error::errctx;
use tidb_model::job::ResolvedTimeZone;
use tidb_model::{ColumnInfo, GoShared};
use tidb_mysql::SqlMode;
use tidb_resolve::ResultField;
use tidb_sqlexec::{ExecutionContext, RecordSet, SimpleRecordSet};
use tidb_util::sqlescape::SqlArg;

use super::{DestroyMode, Error, Pool, ResourcePool, Result, Session, SessionContext, Transaction};

#[derive(Debug)]
struct MockTxn {
    valid: AtomicBool,
    start_ts: u64,
}

impl Transaction for MockTxn {
    fn valid(&self) -> bool {
        self.valid.load(Ordering::SeqCst)
    }

    fn start_ts(&self) -> u64 {
        self.start_ts
    }
}

#[derive(Default)]
struct LockState {
    owner: Option<u64>,
}

struct MockContext {
    id: u64,
    next_ts: Arc<AtomicU64>,
    txn: Mutex<Option<Arc<MockTxn>>>,
    internal_start_ts: Arc<Mutex<HashSet<u64>>>,
    lock: Arc<(Mutex<LockState>, Condvar)>,
    owns_lock: AtomicBool,
    registered: AtomicBool,
    closed: AtomicBool,
    disk_option: AtomicBool,
    autocommit: AtomicBool,
    restricted: AtomicBool,
    timezone_set: AtomicBool,
    schedule_sql_mode: Mutex<Option<SqlMode>>,
    schedule_zone: Mutex<Option<String>>,
    schedule_originals: Mutex<Option<ScheduleEvalOriginals>>,
    schedule_eval_result: Mutex<Option<std::result::Result<Option<tidb_datatype::Time>, String>>>,
    schedule_eval_calls: Mutex<Vec<String>>,
}

impl MockContext {
    fn new(
        id: u64,
        next_ts: Arc<AtomicU64>,
        internal_start_ts: Arc<Mutex<HashSet<u64>>>,
        lock: Arc<(Mutex<LockState>, Condvar)>,
    ) -> Self {
        Self {
            id,
            next_ts,
            txn: Mutex::new(None),
            internal_start_ts,
            lock,
            owns_lock: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            disk_option: AtomicBool::new(false),
            autocommit: AtomicBool::new(false),
            restricted: AtomicBool::new(false),
            timezone_set: AtomicBool::new(false),
            schedule_sql_mode: Mutex::new(None),
            schedule_zone: Mutex::new(None),
            schedule_originals: Mutex::new(None),
            schedule_eval_result: Mutex::new(None),
            schedule_eval_calls: Mutex::new(Vec::new()),
        }
    }

    fn start(&self) {
        let start_ts = self.next_ts.fetch_add(1, Ordering::SeqCst);
        let transaction = Arc::new(MockTxn {
            valid: AtomicBool::new(true),
            start_ts,
        });
        *self.txn.lock().unwrap() = Some(transaction);
        if self.registered.load(Ordering::SeqCst) {
            self.internal_start_ts.lock().unwrap().insert(start_ts);
        }
    }

    fn finish(&self) {
        if let Some(txn) = self.txn.lock().unwrap().take() {
            txn.valid.store(false, Ordering::SeqCst);
            self.internal_start_ts.lock().unwrap().remove(&txn.start_ts);
        }
        if self.owns_lock.swap(false, Ordering::SeqCst) {
            let (lock, ready) = &*self.lock;
            lock.lock().unwrap().owner = None;
            ready.notify_all();
        }
    }

    fn fields() -> Vec<GoShared<ResultField>> {
        vec![GoShared::new(ResultField {
            column: Some(GoShared::new(ColumnInfo {
                field_type: FieldType::new(FieldTypeCode::LongLong),
                ..ColumnInfo::default()
            })),
            ..ResultField::default()
        })]
    }
}

impl SessionContext for MockContext {
    fn new_txn(&self, _context: &dyn ExecutionContext) -> Result<()> {
        self.start();
        Ok(())
    }

    fn enter_new_pessimistic_txn(&self, _context: &dyn ExecutionContext) -> Result<()> {
        self.start();
        Ok(())
    }

    fn set_in_txn(&self, _in_txn: bool) {}

    fn stmt_commit(&self, _context: &dyn ExecutionContext) {}

    fn commit_txn(&self, _context: &dyn ExecutionContext) -> Result<()> {
        self.finish();
        Ok(())
    }

    fn txn(&self, active: bool) -> Result<Option<Arc<dyn Transaction>>> {
        if active && self.txn.lock().unwrap().is_none() {
            self.start();
        }
        Ok(self
            .txn
            .lock()
            .unwrap()
            .as_ref()
            .map(|txn| Arc::clone(txn) as Arc<dyn Transaction>))
    }

    fn stmt_rollback(&self, _context: &dyn ExecutionContext, _is_pessimistic_retry: bool) {}

    fn rollback_txn(&self, _context: &dyn ExecutionContext) {
        self.finish();
    }

    fn request_source(&self, _context: &dyn ExecutionContext) -> Option<String> {
        None
    }

    fn execute_internal(
        &self,
        _context: &dyn ExecutionContext,
        request_source: &str,
        query: &str,
        _arguments: &[SqlArg<'_>],
    ) -> std::result::Result<Option<Box<dyn RecordSet>>, tidb_sqlexec::SqlExecError> {
        assert_eq!(request_source, "ddl");
        if query == "select 2" {
            return Ok(Some(Box::new(SimpleRecordSet::new(
                Self::fields(),
                vec![vec![Datum::new_int(2)]],
                32,
            ))));
        }
        if query.starts_with("update ") {
            let (lock, ready) = &*self.lock;
            let mut state = lock.lock().unwrap();
            while state.owner.is_some_and(|owner| owner != self.id) {
                state = ready.wait(state).unwrap();
            }
            state.owner = Some(self.id);
            self.owns_lock.store(true, Ordering::SeqCst);
            return Ok(None);
        }
        Err(std::io::Error::other(format!("unexpected SQL: {query}")).into())
    }

    fn set_autocommit(&self, enabled: bool) {
        self.autocommit.store(enabled, Ordering::SeqCst);
    }

    fn set_restricted_sql(&self, enabled: bool) {
        self.restricted.store(enabled, Ordering::SeqCst);
    }

    fn set_statement_timezone_to_session_location(&self) {
        self.timezone_set.store(true, Ordering::SeqCst);
    }

    fn allow_on_almost_full(&self) {
        self.disk_option.store(true, Ordering::SeqCst);
    }

    fn clear_disk_full_option(&self) {
        self.disk_option.store(false, Ordering::SeqCst);
    }

    fn register_internal_session(&self) {
        self.registered.store(true, Ordering::SeqCst);
        if let Some(txn) = self.txn.lock().unwrap().as_ref() {
            self.internal_start_ts.lock().unwrap().insert(txn.start_ts);
        }
    }

    fn unregister_internal_session(&self) {
        self.registered.store(false, Ordering::SeqCst);
        if let Some(txn) = self.txn.lock().unwrap().as_ref() {
            self.internal_start_ts.lock().unwrap().remove(&txn.start_ts);
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
    fn install_schedule_eval_session(
        &self,
        sql_mode: SqlMode,
        zone: &ResolvedTimeZone,
    ) -> ScheduleEvalOriginals {
        *self.schedule_sql_mode.lock().unwrap() = Some(sql_mode);
        *self.schedule_zone.lock().unwrap() = Some(zone.name());
        let originals = ScheduleEvalOriginals {
            sql_mode: SqlMode::default(),
            stmt_type_flags: tidb_datatype::ConversionFlags::default(),
            stmt_err_levels: tidb_error::errctx::LevelMap::strict(),
            session_time_zone: None,
            stmt_time_zone: None,
        };
        *self.schedule_originals.lock().unwrap() = Some(originals.clone());
        originals
    }

    fn restore_schedule_eval_session(&self, originals: &ScheduleEvalOriginals) {
        *self.schedule_originals.lock().unwrap() = Some(originals.clone());
        *self.schedule_sql_mode.lock().unwrap() = Some(originals.sql_mode);
    }

    fn eval_schedule_expression(&self, expr_sql: &str) -> Result<Option<tidb_datatype::Time>> {
        self.schedule_eval_calls
            .lock()
            .unwrap()
            .push(expr_sql.to_owned());
        let slot = self.schedule_eval_result.lock().unwrap().take();
        match slot {
            Some(Ok(value)) => Ok(value),
            Some(Err(message)) => Err(Error::new(message)),
            None => Err(Error::new("no schedule eval result configured")),
        }
    }
}

struct MockPool {
    resources: Mutex<VecDeque<Arc<MockContext>>>,
    factory: Arc<dyn Fn() -> Arc<MockContext> + Send + Sync>,
    mode: DestroyMode,
    puts: AtomicUsize,
    destroys: AtomicUsize,
    closed: AtomicBool,
}

impl MockPool {
    fn new(
        initial: Vec<Arc<MockContext>>,
        factory: impl Fn() -> Arc<MockContext> + Send + Sync + 'static,
        mode: DestroyMode,
    ) -> Self {
        Self {
            resources: Mutex::new(initial.into()),
            factory: Arc::new(factory),
            mode,
            puts: AtomicUsize::new(0),
            destroys: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }
    }
}

impl ResourcePool<MockContext> for Arc<MockPool> {
    fn get(&self) -> Result<Arc<MockContext>> {
        Ok(self
            .resources
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| (self.factory)()))
    }

    fn put(&self, resource: Option<Arc<MockContext>>) {
        self.puts.fetch_add(1, Ordering::SeqCst);
        if let Some(resource) = resource {
            self.resources.lock().unwrap().push_back(resource);
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    fn destroy_mode(&self) -> DestroyMode {
        self.mode
    }

    fn destroy(&self, _resource: Arc<MockContext>) {
        self.destroys.fetch_add(1, Ordering::SeqCst);
    }
}

type Fixture = (
    Arc<AtomicU64>,
    Arc<Mutex<HashSet<u64>>>,
    Arc<(Mutex<LockState>, Condvar)>,
);

fn fixture() -> Fixture {
    (
        Arc::new(AtomicU64::new(1)),
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new((Mutex::new(LockState::default()), Condvar::new())),
    )
}

fn context(id: u64, fixture: &Fixture) -> Arc<MockContext> {
    Arc::new(MockContext::new(
        id,
        Arc::clone(&fixture.0),
        Arc::clone(&fixture.1),
        Arc::clone(&fixture.2),
    ))
}

#[test]
fn session_pool() {
    let fixture = fixture();
    let first = context(1, &fixture);
    let pool = Pool::new(Arc::new(MockPool::new(
        vec![Arc::clone(&first)],
        {
            let first = Arc::clone(&first);
            move || Arc::clone(&first)
        },
        DestroyMode::ResourcePool,
    )));
    let context = pool.get().unwrap();
    assert!(context.autocommit.load(Ordering::SeqCst));
    assert!(context.restricted.load(Ordering::SeqCst));
    assert!(context.timezone_set.load(Ordering::SeqCst));
    assert!(context.disk_option.load(Ordering::SeqCst));
    let session = Session::new(Arc::clone(&context));
    session.begin(&tidb_sqlexec::BackgroundContext).unwrap();
    let start_ts = session.txn().unwrap().unwrap().start_ts();
    assert!(fixture.1.lock().unwrap().contains(&start_ts));
    let rows = session
        .execute(&tidb_sqlexec::BackgroundContext, "select 2", "test", &[])
        .unwrap()
        .unwrap();
    assert_eq!(rows, vec![vec![Datum::new_int(2)]]);
    session.commit(&tidb_sqlexec::BackgroundContext).unwrap();
    pool.put(context);
    assert!(!fixture.1.lock().unwrap().contains(&start_ts));
}

#[test]
fn pessimistic_txn() {
    let fixture = fixture();
    let first = context(1, &fixture);
    let second = context(2, &fixture);
    let pool = Arc::new(Pool::new(Arc::new(MockPool::new(
        vec![Arc::clone(&first), Arc::clone(&second)],
        {
            let first = Arc::clone(&first);
            move || Arc::clone(&first)
        },
        DestroyMode::ResourcePool,
    ))));
    let first = pool.get().unwrap();
    let second = pool.get().unwrap();
    let first_session = Session::new(Arc::clone(&first));
    first_session
        .begin_pessimistic(&tidb_sqlexec::BackgroundContext)
        .unwrap();
    first_session
        .execute(
            &tidb_sqlexec::BackgroundContext,
            "update t set b = b + 1",
            "test",
            &[],
        )
        .unwrap();

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let second_session = Session::new(second);
        second_session
            .begin_pessimistic(&tidb_sqlexec::BackgroundContext)
            .unwrap();
        second_session
            .execute(
                &tidb_sqlexec::BackgroundContext,
                "update t set b = b + 1",
                "test",
                &[],
            )
            .unwrap();
        second_session
            .commit(&tidb_sqlexec::BackgroundContext)
            .unwrap();
        done_tx.send(()).unwrap();
    });
    assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
    first_session
        .commit(&tidb_sqlexec::BackgroundContext)
        .unwrap();
    done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    worker.join().unwrap();
    pool.put(first);
}

#[test]
fn session_pool_destroy_resource_pool() {
    let fixture = fixture();
    let first = context(1, &fixture);
    let next_id = Arc::new(AtomicU64::new(2));
    let underlying = Arc::new(MockPool::new(
        vec![Arc::clone(&first)],
        {
            let fixture = fixture;
            let next_id = Arc::clone(&next_id);
            move || context(next_id.fetch_add(1, Ordering::SeqCst), &fixture)
        },
        DestroyMode::ResourcePool,
    ));
    let pool = Pool::new(Arc::clone(&underlying));
    let checked_out = pool.get().unwrap();
    pool.destroy(checked_out);
    assert!(first.closed.load(Ordering::SeqCst));
    let replacement = pool.get().unwrap();
    assert!(!Arc::ptr_eq(&first, &replacement));
    assert_eq!(underlying.puts.load(Ordering::SeqCst), 1);
}

#[test]
fn session_pool_destroy_destroyable_session_pool() {
    let fixture = fixture();
    let first = context(1, &fixture);
    let underlying = Arc::new(MockPool::new(
        vec![Arc::clone(&first)],
        {
            let first = Arc::clone(&first);
            move || Arc::clone(&first)
        },
        DestroyMode::Destroyable,
    ));
    let pool = Pool::new(Arc::clone(&underlying));
    pool.destroy(pool.get().unwrap());
    assert_eq!(underlying.destroys.load(Ordering::SeqCst), 1);
    assert_eq!(underlying.puts.load(Ordering::SeqCst), 0);
}

#[test]
fn closed_pool_returns_exact_error() {
    let fixture = fixture();
    let first = context(1, &fixture);
    let underlying = Arc::new(MockPool::new(
        vec![Arc::clone(&first)],
        move || Arc::clone(&first),
        DestroyMode::ResourcePool,
    ));
    let pool = Pool::new(Arc::clone(&underlying));
    pool.close();
    pool.close();
    let error = match pool.get() {
        Ok(_) => panic!("closed pool returned a session"),
        Err(error) => error,
    };
    assert_eq!(error, Error::new("session pool is closed"));
    assert!(underlying.closed.load(Ordering::SeqCst));
}

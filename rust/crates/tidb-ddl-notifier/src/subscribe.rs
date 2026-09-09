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

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::SchemaChangeEvent;

/// Go `notifier.ErrNotReadyRetryLater` plus ordinary handler/store failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NotifierError {
    /// The handler is registered but cannot consume this event yet.
    NotReadyRetryLater,
    /// Any other operation failure.
    Message(String),
}

impl fmt::Display for NotifierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotReadyRetryLater => formatter.write_str("not ready, retry later"),
            Self::Message(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for NotifierError {}

impl From<String> for NotifierError {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for NotifierError {
    fn from(value: &str) -> Self {
        Self::Message(value.to_owned())
    }
}

/// One internal SQL session borrowed by the notifier.
pub trait NotifierSession: Any + Send {
    /// Allows the concrete store/handler adapter to reach its session type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// Starts the pessimistic transaction used for one handler and flag CAS.
    fn begin_pessimistic(&mut self) -> Result<(), NotifierError>;
    /// Commits the active transaction.
    fn commit(&mut self) -> Result<(), NotifierError>;
    /// Rolls back the active transaction.
    fn rollback(&mut self);
}

/// Go `util.SessionPool` subset used by the notifier.
pub trait SessionPool: Send + Sync + 'static {
    /// Checks out one system session.
    fn get(&self) -> Result<Box<dyn NotifierSession>, NotifierError>;
    /// Returns a checked-out system session.
    fn put(&self, session: Box<dyn NotifierSession>);
}

/// Go `notifier.SchemaChange`.
#[derive(Clone, Debug)]
pub struct SchemaChange {
    /// DDL job ID; positive and cluster-global.
    pub ddl_job_id: i64,
    /// `-1` for an ordinary job or the sub-job index.
    pub sub_job_id: i64,
    /// Serialized schema-change payload.
    pub event: SchemaChangeEvent,
    /// Persistent exactly-once subscriber bitmap.
    pub processed_by_flag: u64,
}

/// One paginated snapshot returned by [`Store::list`].
pub trait ListResult: Send {
    /// Decodes at most `capacity` events; an empty vector means EOF.
    fn read(
        &mut self,
        session: &mut dyn NotifierSession,
        capacity: usize,
    ) -> Result<Vec<SchemaChange>, NotifierError>;
    /// Releases the list transaction (Go's `CloseFn`, normally rollback).
    fn close(self: Box<Self>, session: &mut dyn NotifierSession);
}

/// Go `notifier.Store`.
pub trait Store: Send + Sync + 'static {
    /// Stages one event in the caller's transaction.
    fn insert(
        &self,
        session: &mut dyn NotifierSession,
        change: &SchemaChange,
    ) -> Result<(), NotifierError>;
    /// CASes one handler's processed bit in the handler transaction.
    fn update_processed(
        &self,
        session: &mut dyn NotifierSession,
        ddl_job_id: i64,
        sub_job_id: i64,
        old_processed_by: u64,
        new_processed_by: u64,
    ) -> Result<(), NotifierError>;
    /// Deletes one fully processed event and commits that deletion.
    fn delete_and_commit(
        &self,
        session: &mut dyn NotifierSession,
        ddl_job_id: i64,
        sub_job_id: i64,
    ) -> Result<(), NotifierError>;
    /// Starts the ordered list snapshot.
    fn list(&self, session: &mut dyn NotifierSession)
        -> Result<Box<dyn ListResult>, NotifierError>;
}

/// Go `PubSchemeChangeToStore` (the misspelling is not carried into Rust).
pub fn publish_schema_change_to_store(
    session: &mut dyn NotifierSession,
    ddl_job_id: i64,
    sub_job_id: i64,
    event: SchemaChangeEvent,
    store: &dyn Store,
) -> Result<(), NotifierError> {
    store.insert(
        session,
        &SchemaChange {
            ddl_job_id,
            sub_job_id,
            event,
            processed_by_flag: 0,
        },
    )
}

/// Go `notifier.HandlerID`.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct HandlerId(pub i32);

/// Go `TestHandlerID`.
pub const TEST_HANDLER_ID: HandlerId = HandlerId(0);
/// Go `StatsMetaHandlerID`.
pub const STATS_META_HANDLER_ID: HandlerId = HandlerId(1);
/// Go `PriorityQueueHandlerID`.
pub const PRIORITY_QUEUE_HANDLER_ID: HandlerId = HandlerId(2);

impl fmt::Display for HandlerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            TEST_HANDLER_ID => formatter.write_str("TestHandler"),
            STATS_META_HANDLER_ID => formatter.write_str("StatsMetaHandler"),
            _ => write!(formatter, "HandlerID({})", self.0),
        }
    }
}

/// Go `SchemaChangeHandler`.
pub type Handler = Arc<
    dyn Fn(&mut dyn NotifierSession, &SchemaChangeEvent) -> Result<(), NotifierError> + Send + Sync,
>;

/// Go `ProcessEventsBatchSize`.
pub static PROCESS_EVENTS_BATCH_SIZE: AtomicUsize = AtomicUsize::new(1_024);

struct Worker {
    stop: mpsc::Sender<()>,
    join: JoinHandle<()>,
}

/// Go `notifier.DDLNotifier`.
pub struct DdlNotifier {
    pool: Arc<dyn SessionPool>,
    store: Arc<dyn Store>,
    handlers: Mutex<BTreeMap<HandlerId, Handler>>,
    poll_interval: Duration,
    worker: Mutex<Option<Worker>>,
}

impl DdlNotifier {
    /// Go `NewDDLNotifier`.
    pub fn new(pool: Arc<dyn SessionPool>, store: Arc<dyn Store>, poll_interval: Duration) -> Self {
        Self {
            pool,
            store,
            handlers: Mutex::new(BTreeMap::new()),
            poll_interval,
            worker: Mutex::new(None),
        }
    }

    /// Go `RegisterHandler`.
    pub fn register_handler(&self, id: HandlerId, handler: Handler) {
        assert!((0..64).contains(&id.0), "illegal HandlerID: {}", id.0);
        let mut handlers = self.handlers.lock().expect("handler lock poisoned");
        if handlers.contains_key(&id) {
            eprintln!(
                "{{\"event\":\"ddl_notifier_handler_already_registered\",\"id\":{}}}",
                id.0
            );
            return;
        }
        handlers.insert(id, handler);
    }

    /// Go `Stop`.
    pub fn stop(&self) {
        let worker = self.worker.lock().expect("worker lock poisoned").take();
        if let Some(worker) = worker {
            let _ = worker.stop.send(());
            let _ = worker.join.join();
        }
    }

    fn start(&self) {
        let mut worker = self.worker.lock().expect("worker lock poisoned");
        if worker.is_some() {
            return;
        }
        let pool = Arc::clone(&self.pool);
        let store = Arc::clone(&self.store);
        let handlers = self.handlers.lock().expect("handler lock poisoned").clone();
        let interval = self.poll_interval;
        let (stop, stopped) = mpsc::channel();
        let join = std::thread::spawn(move || loop {
            match stopped.recv_timeout(interval) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(error) = process_events(pool.as_ref(), store.as_ref(), &handlers) {
                        eprintln!(
                            "{{\"event\":\"ddl_notifier_process_failed\",\"error\":{error:?}}}"
                        );
                    }
                }
            }
        });
        *worker = Some(Worker { stop, join });
    }
}

impl Drop for DdlNotifier {
    fn drop(&mut self) {
        self.stop();
    }
}

impl tidb_owner::Listener for DdlNotifier {
    fn on_become_owner(&self) {
        self.start();
    }

    fn on_retire_owner(&self) {
        self.stop();
    }
}

fn process_events(
    pool: &dyn SessionPool,
    store: &dyn Store,
    handlers: &BTreeMap<HandlerId, Handler>,
) -> Result<(), NotifierError> {
    let handlers_bitmap = handlers
        .keys()
        .fold(0_u64, |bitmap, id| bitmap | (1_u64 << id.0));
    let mut list_session = pool.get()?;
    let mut result = match store.list(list_session.as_mut()) {
        Ok(result) => result,
        Err(error) => {
            pool.put(list_session);
            return Err(error);
        }
    };
    let mut process_session = match pool.get() {
        Ok(session) => session,
        Err(error) => {
            result.close(list_session.as_mut());
            pool.put(list_session);
            return Err(error);
        }
    };
    let mut skipped = BTreeSet::new();
    let outcome = (|| {
        loop {
            let changes = result.read(
                list_session.as_mut(),
                PROCESS_EVENTS_BATCH_SIZE.load(Ordering::Relaxed),
            )?;
            if changes.is_empty() {
                break;
            }
            for mut change in changes {
                for (id, handler) in handlers {
                    if skipped.contains(id) {
                        continue;
                    }
                    if let Err(error) = process_event_for_handler(
                        store,
                        process_session.as_mut(),
                        &mut change,
                        *id,
                        handler,
                    ) {
                        skipped.insert(*id);
                        if error != NotifierError::NotReadyRetryLater {
                            eprintln!(
                                "{{\"event\":\"ddl_notifier_handler_failed\",\"ddl_job_id\":{},\"sub_job_id\":{},\"handler\":\"{}\",\"error\":{error:?}}}",
                                change.ddl_job_id, change.sub_job_id, id
                            );
                        }
                    }
                }
                if change.processed_by_flag == handlers_bitmap {
                    let mut delete_session = pool.get()?;
                    let deleted = store.delete_and_commit(
                        delete_session.as_mut(),
                        change.ddl_job_id,
                        change.sub_job_id,
                    );
                    pool.put(delete_session);
                    if let Err(error) = deleted {
                        eprintln!(
                            "{{\"event\":\"ddl_notifier_delete_failed\",\"ddl_job_id\":{},\"sub_job_id\":{},\"error\":{error:?}}}",
                            change.ddl_job_id, change.sub_job_id
                        );
                    }
                }
            }
        }
        Ok(())
    })();
    result.close(list_session.as_mut());
    pool.put(process_session);
    pool.put(list_session);
    outcome
}

fn process_event_for_handler(
    store: &dyn Store,
    session: &mut dyn NotifierSession,
    change: &mut SchemaChange,
    id: HandlerId,
    handler: &Handler,
) -> Result<(), NotifierError> {
    let bit = 1_u64 << id.0;
    if change.processed_by_flag & bit != 0 {
        return Ok(());
    }
    let new_flag = change.processed_by_flag | bit;
    session.begin_pessimistic()?;
    let result = handler(session, &change.event).and_then(|()| {
        store.update_processed(
            session,
            change.ddl_job_id,
            change.sub_job_id,
            change.processed_by_flag,
            new_flag,
        )
    });
    if let Err(error) = result {
        session.rollback();
        return Err(error);
    }
    if let Err(error) = session.commit() {
        return Err(error);
    }
    change.processed_by_flag = new_flag;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tidb_ast::CiString;
    use tidb_model::TableInfo;

    #[derive(Default)]
    struct MockSession {
        active: bool,
        fail_commit: bool,
        staged: Vec<Box<dyn FnOnce() + Send>>,
    }

    impl NotifierSession for MockSession {
        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn begin_pessimistic(&mut self) -> Result<(), NotifierError> {
            self.active = true;
            Ok(())
        }

        fn commit(&mut self) -> Result<(), NotifierError> {
            if self.fail_commit {
                self.active = false;
                self.staged.clear();
                return Err("mock commit failed".into());
            }
            self.active = false;
            for mutation in std::mem::take(&mut self.staged) {
                mutation();
            }
            Ok(())
        }

        fn rollback(&mut self) {
            self.active = false;
            self.staged.clear();
        }
    }

    #[derive(Default)]
    struct MockPool;

    impl SessionPool for MockPool {
        fn get(&self) -> Result<Box<dyn NotifierSession>, NotifierError> {
            Ok(Box::<MockSession>::default())
        }

        fn put(&self, _session: Box<dyn NotifierSession>) {}
    }

    #[derive(Default)]
    struct MockStore {
        rows: Arc<Mutex<BTreeMap<(i64, i64), SchemaChange>>>,
    }

    struct MockList {
        rows: Vec<SchemaChange>,
        offset: usize,
    }

    impl ListResult for MockList {
        fn read(
            &mut self,
            _session: &mut dyn NotifierSession,
            capacity: usize,
        ) -> Result<Vec<SchemaChange>, NotifierError> {
            let end = (self.offset + capacity).min(self.rows.len());
            let rows = self.rows[self.offset..end].to_vec();
            self.offset = end;
            Ok(rows)
        }

        fn close(self: Box<Self>, _session: &mut dyn NotifierSession) {}
    }

    impl Store for MockStore {
        fn insert(
            &self,
            _session: &mut dyn NotifierSession,
            change: &SchemaChange,
        ) -> Result<(), NotifierError> {
            self.rows
                .lock()
                .unwrap()
                .insert((change.ddl_job_id, change.sub_job_id), change.clone());
            Ok(())
        }

        fn update_processed(
            &self,
            session: &mut dyn NotifierSession,
            ddl_job_id: i64,
            sub_job_id: i64,
            old_processed_by: u64,
            new_processed_by: u64,
        ) -> Result<(), NotifierError> {
            let rows = Arc::clone(&self.rows);
            let session = session.as_any_mut().downcast_mut::<MockSession>().unwrap();
            let current = rows
                .lock()
                .unwrap()
                .get(&(ddl_job_id, sub_job_id))
                .map(|row| row.processed_by_flag);
            if current != Some(old_processed_by) {
                return Err("failed to update processed_by_flag, maybe the row has been updated by other owner".into());
            }
            session.staged.push(Box::new(move || {
                rows.lock()
                    .unwrap()
                    .get_mut(&(ddl_job_id, sub_job_id))
                    .unwrap()
                    .processed_by_flag = new_processed_by;
            }));
            Ok(())
        }

        fn delete_and_commit(
            &self,
            _session: &mut dyn NotifierSession,
            ddl_job_id: i64,
            sub_job_id: i64,
        ) -> Result<(), NotifierError> {
            self.rows.lock().unwrap().remove(&(ddl_job_id, sub_job_id));
            Ok(())
        }

        fn list(
            &self,
            _session: &mut dyn NotifierSession,
        ) -> Result<Box<dyn ListResult>, NotifierError> {
            Ok(Box::new(MockList {
                rows: self.rows.lock().unwrap().values().cloned().collect(),
                offset: 0,
            }))
        }
    }

    fn event(id: i64) -> SchemaChangeEvent {
        SchemaChangeEvent::create_table(TableInfo {
            id,
            name: CiString::new(format!("t{id}")),
            ..TableInfo::default()
        })
    }

    #[test]
    #[deny(unused_must_use)]
    fn go_notifier_constructor_may_be_ignored_like_go() {
        let pool: Arc<dyn SessionPool> = Arc::new(MockPool);
        let store: Arc<dyn Store> = Arc::new(MockStore::default());
        DdlNotifier::new(pool, store, Duration::from_millis(1));
    }

    #[test]
    fn retries_in_order_and_cleans_up_after_every_handler() {
        let pool: Arc<dyn SessionPool> = Arc::new(MockPool);
        let store = Arc::new(MockStore::default());
        let notifier = DdlNotifier::new(
            Arc::clone(&pool),
            Arc::clone(&store) as Arc<dyn Store>,
            Duration::from_millis(10),
        );
        let attempts = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler: Handler = {
            let attempts = Arc::clone(&attempts);
            let seen = Arc::clone(&seen);
            Arc::new(move |_, event| {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(NotifierError::NotReadyRetryLater);
                }
                seen.lock().unwrap().push(event.create_table_info().id);
                Ok(())
            })
        };
        notifier.register_handler(TEST_HANDLER_ID, handler);
        let mut publish = pool.get().unwrap();
        for id in 1..=3 {
            publish_schema_change_to_store(publish.as_mut(), id, -1, event(id), store.as_ref())
                .unwrap();
        }
        pool.put(publish);

        let handlers = notifier.handlers.lock().unwrap().clone();
        process_events(pool.as_ref(), store.as_ref(), &handlers).unwrap();
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(store.rows.lock().unwrap().len(), 3);
        process_events(pool.as_ref(), store.as_ref(), &handlers).unwrap();
        assert_eq!(*seen.lock().unwrap(), [1, 2, 3]);
        assert!(store.rows.lock().unwrap().is_empty());
    }

    #[test]
    fn one_failing_handler_does_not_lose_another_handlers_bit() {
        let pool: Arc<dyn SessionPool> = Arc::new(MockPool);
        let store = Arc::new(MockStore::default());
        let notifier = DdlNotifier::new(
            Arc::clone(&pool),
            Arc::clone(&store) as Arc<dyn Store>,
            Duration::from_millis(10),
        );
        notifier.register_handler(
            HandlerId(1),
            Arc::new(|_, _| Err(NotifierError::NotReadyRetryLater)),
        );
        notifier.register_handler(HandlerId(2), Arc::new(|_, _| Ok(())));
        let mut publish = pool.get().unwrap();
        publish_schema_change_to_store(publish.as_mut(), 1, -1, event(1), store.as_ref()).unwrap();
        pool.put(publish);

        let handlers = notifier.handlers.lock().unwrap().clone();
        process_events(pool.as_ref(), store.as_ref(), &handlers).unwrap();
        let rows = store.rows.lock().unwrap();
        assert_eq!(rows[&(1, -1)].processed_by_flag, 1 << 2);
    }

    #[test]
    fn commit_failure_keeps_the_processed_bit_and_handler_write_uncommitted() {
        let store = MockStore::default();
        let mut publish = MockSession::default();
        publish_schema_change_to_store(&mut publish, 1, -1, event(1), &store).unwrap();
        let handler_writes = Arc::new(AtomicUsize::new(0));
        let handler: Handler = {
            let handler_writes = Arc::clone(&handler_writes);
            Arc::new(move |session, _| {
                let session = session.as_any_mut().downcast_mut::<MockSession>().unwrap();
                session.staged.push(Box::new({
                    let handler_writes = Arc::clone(&handler_writes);
                    move || {
                        handler_writes.fetch_add(1, Ordering::SeqCst);
                    }
                }));
                Ok(())
            })
        };
        let mut change = store.rows.lock().unwrap()[&(1, -1)].clone();
        let mut session = MockSession {
            fail_commit: true,
            ..MockSession::default()
        };

        assert_eq!(
            process_event_for_handler(&store, &mut session, &mut change, TEST_HANDLER_ID, &handler),
            Err(NotifierError::Message("mock commit failed".to_owned()))
        );
        assert_eq!(handler_writes.load(Ordering::SeqCst), 0);
        assert_eq!(store.rows.lock().unwrap()[&(1, -1)].processed_by_flag, 0);
        assert_eq!(change.processed_by_flag, 0);
        assert!(!session.active);
        assert!(session.staged.is_empty());
    }

    #[test]
    fn competing_owner_cas_loss_rolls_back_handler_write() {
        let store = MockStore::default();
        let mut publish = MockSession::default();
        publish_schema_change_to_store(&mut publish, 1, -1, event(1), &store).unwrap();
        let handler_writes = Arc::new(AtomicUsize::new(0));
        let rows = Arc::clone(&store.rows);
        let handler: Handler = {
            let handler_writes = Arc::clone(&handler_writes);
            Arc::new(move |session, _| {
                let session = session.as_any_mut().downcast_mut::<MockSession>().unwrap();
                session.staged.push(Box::new({
                    let handler_writes = Arc::clone(&handler_writes);
                    move || {
                        handler_writes.fetch_add(1, Ordering::SeqCst);
                    }
                }));
                rows.lock().unwrap().remove(&(1, -1));
                Ok(())
            })
        };
        let mut change = store.rows.lock().unwrap()[&(1, -1)].clone();
        let mut session = MockSession::default();

        let error =
            process_event_for_handler(&store, &mut session, &mut change, TEST_HANDLER_ID, &handler)
                .expect_err("the losing owner must fail its processed-bit CAS");
        assert!(error.to_string().contains("updated by other owner"));
        assert_eq!(handler_writes.load(Ordering::SeqCst), 0);
        assert_eq!(change.processed_by_flag, 0);
        assert!(!session.active);
        assert!(session.staged.is_empty());
    }

    #[test]
    fn list_pages_in_job_and_sub_job_order() {
        let store = MockStore::default();
        let mut session = MockSession::default();
        for (job_id, sub_job_id) in [(2, -1), (1, 2), (1, -1), (1, 1), (3, -1)] {
            store
                .insert(
                    &mut session,
                    &SchemaChange {
                        ddl_job_id: job_id,
                        sub_job_id,
                        event: event(job_id),
                        processed_by_flag: 0,
                    },
                )
                .unwrap();
        }
        let mut list = store.list(&mut session).unwrap();
        let first = list.read(&mut session, 3).unwrap();
        let second = list.read(&mut session, 3).unwrap();
        assert_eq!(
            first
                .iter()
                .chain(&second)
                .map(|change| (change.ddl_job_id, change.sub_job_id))
                .collect::<Vec<_>>(),
            [(1, -1), (1, 1), (1, 2), (2, -1), (3, -1)]
        );
        list.close(&mut session);
    }
}

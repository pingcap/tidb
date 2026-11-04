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

//! Go `manager.go`: detects and records runaway queries.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, Utc};
use tidb_proto::resource_manager as rmpb;

use crate::checker::Checker;
use crate::flusher::BatchFlusher;
use crate::record::{
    gen_batch_delete_watch_by_id_stmt, gen_batch_insert_watch_stmt, gen_runaway_queries_stmt, null_time,
    QuarantineRecord, Record, RecordKey,
};
use crate::syncer::{Syncer, WATCH_SYNC_INTERVAL, WATCH_SYNC_OVERLAP};
use crate::ttlcache::{Cache, EvictionReason};
use crate::{DdlOwner, InfoCache, ResourceGroupsController, SessionPool};

/// Go `ManualSource`: the item added manually.
pub const MANUAL_SOURCE: &str = "manual";

/// Go `MaxWaitDuration`: the max duration to wait for acquiring token
/// buckets.
pub const MAX_WAIT_DURATION: Duration = Duration::from_secs(30);
const MAX_WATCH_LIST_CAP: usize = 10_000;
pub(crate) const MAX_WATCH_RECORD_CHANNEL_SIZE: usize = 1024;

const RUNAWAY_RECORD_FLUSH_INTERVAL: Duration = Duration::from_secs(30);
const RUNAWAY_RECORD_GC_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const RUNAWAY_RECORD_EXPIRED_DURATION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

pub(crate) const RUNAWAY_RECORD_GC_BATCH_SIZE: usize = 100;
pub(crate) const RUNAWAY_RECORD_GC_SELECT_BATCH_SIZE: usize = RUNAWAY_RECORD_GC_BATCH_SIZE * 5;

const WATCH_RECORD_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Go's three record channels, each buffered to
/// `maxWatchRecordChannelSize`: a full channel drops a `select`-with-default
/// send and blocks a plain one.
#[derive(Default)]
struct RecordQueues {
    runaway_queries: VecDeque<Record>,
    quarantine: VecDeque<QuarantineRecord>,
    stale_quarantine_record: VecDeque<QuarantineRecord>,
    exit: bool,
}

#[derive(Default)]
struct RecordChannels {
    queues: Mutex<RecordQueues>,
    /// A send happened (or exit was closed).
    readable: Condvar,
    /// A receive happened.
    writable: Condvar,
}

impl RecordChannels {
    fn lock(&self) -> std::sync::MutexGuard<'_, RecordQueues> {
        self.queues.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `rm.staleQuarantineRecord <- record`: blocks while full.
    fn send_stale(&self, record: QuarantineRecord) {
        let mut queues = self.lock();
        while queues.stale_quarantine_record.len() >= MAX_WATCH_RECORD_CHANNEL_SIZE && !queues.exit {
            queues = self.writable.wait(queues).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        queues.stale_quarantine_record.push_back(record);
        drop(queues);
        self.readable.notify_all();
    }
}

/// Go `Manager.activeGroup`: the active watch count per resource group.
#[derive(Default)]
struct ActiveGroups(RwLock<HashMap<String, Arc<AtomicI64>>>);

impl ActiveGroups {
    /// Go `loadOrStoreActiveCounter`.
    fn load_or_store_active_counter(&self, name: &str) -> Arc<AtomicI64> {
        if let Some(counter) = self.0.read().unwrap_or_else(std::sync::PoisonError::into_inner).get(name) {
            return Arc::clone(counter);
        }
        Arc::clone(
            self.0
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(name.to_owned())
                .or_default(),
        )
    }

    /// Go `getActiveWatchCount`.
    fn get_active_watch_count(&self, name: &str) -> i64 {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .map_or(0, |counter| counter.load(Ordering::SeqCst))
    }
}

/// Go `Manager`: detects and records runaway queries.
pub struct Manager {
    /// Go `exit`, the domain's.
    exit: Arc<ExitSignal>,
    /// Go `queryLock`: makes "is the record in the watch list" and "add it"
    /// one step, since the record also goes to the system table.
    query_lock: Mutex<()>,
    watch_list: Arc<Cache<QuarantineRecord>>,
    active_group: Arc<ActiveGroups>,

    /// Go `ResourceGroupCtl`.
    pub resource_group_ctl: Arc<dyn ResourceGroupsController>,
    server_id: String,
    channels: Arc<RecordChannels>,

    // domain related fields
    pub(crate) info_cache: Arc<dyn InfoCache>,
    pub(crate) ddl: Arc<dyn DdlOwner>,

    /// Go `runawaySyncer`: syncs runaway watch records.
    pub(crate) runaway_syncer: Syncer,
    pub(crate) sys_session_pool: Arc<dyn SessionPool>,
}

/// Go's `exit chan struct{}`, closed once.
#[derive(Default)]
pub struct ExitSignal {
    closed: Mutex<bool>,
    wake: Condvar,
}

impl ExitSignal {
    /// Go `close(exit)`.
    pub fn close(&self) {
        *self.closed.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.wake.notify_all();
    }

    /// Waits up to `timeout`; whether the channel is closed.
    pub fn wait(&self, timeout: Duration) -> bool {
        let closed = self.closed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (closed, _) = self
            .wake
            .wait_timeout_while(closed, timeout, |closed| !*closed)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *closed
    }

    /// Whether the channel is closed.
    pub fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Manager {
    /// Go `NewRunawayManager`; Go's `go watchList.Start()` is the returned
    /// manager's [`Self::watch_list_start`], which the caller runs on a
    /// thread of its own.
    pub fn new(
        resource_group_ctl: Arc<dyn ResourceGroupsController>,
        server_addr: &str,
        pool: Arc<dyn SessionPool>,
        exit: Arc<ExitSignal>,
        info_cache: Arc<dyn InfoCache>,
        ddl: Arc<dyn DdlOwner>,
    ) -> Arc<Self> {
        let watch_list = Arc::new(Cache::new(MAX_WATCH_LIST_CAP));
        let active_group = Arc::new(ActiveGroups::default());
        let channels = Arc::new(RecordChannels::default());
        {
            let active_group = Arc::clone(&active_group);
            watch_list.on_insertion(move |record: &QuarantineRecord| {
                active_group.load_or_store_active_counter(&record.resource_group_name).fetch_add(1, Ordering::SeqCst);
            });
        }
        {
            let active_group = Arc::clone(&active_group);
            let channels = Arc::clone(&channels);
            watch_list.on_eviction(move |_reason: EvictionReason, record: &QuarantineRecord| {
                active_group.load_or_store_active_counter(&record.resource_group_name).fetch_sub(1, Ordering::SeqCst);
                if record.id == 0 {
                    return;
                }
                channels.send_stale(record.clone());
            });
        }
        let runaway_syncer = Syncer::new(Arc::clone(&pool), Arc::clone(&info_cache));
        Arc::new(Self {
            exit,
            query_lock: Mutex::new(()),
            watch_list,
            active_group,
            resource_group_ctl,
            server_id: server_addr.to_owned(),
            channels,
            info_cache,
            ddl,
            runaway_syncer,
            sys_session_pool: pool,
        })
    }

    /// Go `go watchList.Start()`: deletes expired watch items until
    /// [`Self::stop`].
    pub fn watch_list_start(&self) {
        self.watch_list.start();
    }

    /// Waits up to `timeout` on the exit channel; whether it closed.
    pub(crate) fn exit_wait(&self, timeout: Duration) -> bool {
        self.exit.wait(timeout)
    }

    /// Go `RunawayRecordFlushLoop`: flushes runaway, quarantine and stale
    /// quarantine records, and deletes expired runaway records daily.
    pub fn runaway_record_flush_loop(self: &Arc<Self>) {
        let batch_size = flush_threshold();
        let mut runaway_record_flusher = BatchFlusher::new(
            "runaway-record",
            RUNAWAY_RECORD_FLUSH_INTERVAL,
            batch_size,
            |map: &mut HashMap<RecordKey, Record>, key, value: Record| {
                if let Some(existing) = map.get_mut(&key) {
                    existing.repeats += 1;
                } else {
                    map.insert(key, value);
                }
            },
            |map| gen_runaway_queries_stmt(map.values()),
            Arc::clone(&self.sys_session_pool),
        );
        let mut quarantine_record_flusher = BatchFlusher::new(
            "quarantine-record",
            WATCH_RECORD_FLUSH_INTERVAL,
            batch_size,
            |map: &mut HashMap<String, QuarantineRecord>, key, value| {
                map.entry(key).or_insert(value);
            },
            |map| gen_batch_insert_watch_stmt(map.values()),
            Arc::clone(&self.sys_session_pool),
        );
        let mut stale_quarantine_flusher = BatchFlusher::new(
            "stale-quarantine-record",
            WATCH_RECORD_FLUSH_INTERVAL,
            batch_size,
            |map: &mut HashMap<i64, QuarantineRecord>, key, value| {
                map.insert(key, value);
            },
            |map| gen_batch_delete_watch_by_id_stmt(map.keys().copied()),
            Arc::clone(&self.sys_session_pool),
        );
        let mut next_gc = Instant::now() + RUNAWAY_RECORD_GC_INTERVAL;
        loop {
            if self.exit.is_closed() {
                runaway_record_flusher.stop();
                quarantine_record_flusher.stop();
                stale_quarantine_flusher.stop();
                tidb_logutil::bg_logger().info("runaway record flush loop exit", &[]);
                return;
            }
            let now = Instant::now();
            if runaway_record_flusher.next_tick() <= now {
                // flush runaway records periodically
                runaway_record_flusher.tick(now);
                continue;
            }
            if quarantine_record_flusher.next_tick() <= now {
                // flush quarantine records periodically
                quarantine_record_flusher.tick(now);
                continue;
            }
            if stale_quarantine_flusher.next_tick() <= now {
                // flush stale quarantine records periodically
                stale_quarantine_flusher.tick(now);
                continue;
            }
            if next_gc <= now {
                // delete expired runaway records periodically
                while next_gc <= now {
                    next_gc += RUNAWAY_RECORD_GC_INTERVAL;
                }
                let manager = Arc::clone(self);
                std::thread::spawn(move || manager.delete_expired_rows(RUNAWAY_RECORD_EXPIRED_DURATION));
                continue;
            }
            let deadline = [
                runaway_record_flusher.next_tick(),
                quarantine_record_flusher.next_tick(),
                stale_quarantine_flusher.next_tick(),
                next_gc,
            ]
            .into_iter()
            .min()
            .expect("four deadlines");
            let mut queues = self.channels.lock();
            if queues.runaway_queries.is_empty()
                && queues.quarantine.is_empty()
                && queues.stale_quarantine_record.is_empty()
            {
                queues = self
                    .channels
                    .readable
                    .wait_timeout(queues, deadline.saturating_duration_since(Instant::now()))
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .0;
            }
            let runaway = queues.runaway_queries.pop_front();
            let quarantine = queues.quarantine.pop_front();
            let stale = queues.stale_quarantine_record.pop_front();
            drop(queues);
            self.channels.writable.notify_all();
            if let Some(r) = runaway {
                // add runaway records to the flusher
                let key = RecordKey {
                    resource_group_name: r.resource_group_name.clone(),
                    sql_digest: r.sql_digest.clone(),
                    plan_digest: r.plan_digest.clone(),
                    match_type: r.match_type.clone(),
                };
                runaway_record_flusher.add(key, r);
            }
            if let Some(r) = quarantine {
                // add quarantine records to the flusher
                quarantine_record_flusher.add(r.get_record_key(), r);
            }
            if let Some(r) = stale {
                // add stale quarantine records to the flusher
                if r.id != 0 {
                    stale_quarantine_flusher.add(r.id, r);
                }
            }
        }
    }

    /// Go `RunawayWatchSyncLoop`: syncs the watch records every
    /// `watchSyncInterval`.
    pub fn runaway_watch_sync_loop(&self) {
        while !self.exit.wait(WATCH_SYNC_INTERVAL) {
            if let Err(error) = self.update_new_and_done_watch() {
                tidb_logutil::bg_logger().warn(
                    "get runaway watch record failed",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error))],
                );
            }
        }
        tidb_logutil::bg_logger().info("runaway watch sync loop exit", &[]);
    }

    /// Go `markQuarantine`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn mark_quarantine(
        &self,
        resource_group_name: &str,
        convict: &str,
        watch_type: rmpb::RunawayWatchType,
        action: rmpb::RunawayAction,
        switch_group_name: &str,
        ttl: Duration,
        now: DateTime<Local>,
        exceed_cause: &str,
    ) {
        let now = now.with_timezone(&Utc);
        let end_time = if ttl.is_zero() { null_time() } else { now + chrono::Duration::from_std(ttl).unwrap_or_default() };
        let record = QuarantineRecord {
            id: 0,
            resource_group_name: resource_group_name.to_owned(),
            start_time: now,
            end_time,
            watch: watch_type,
            watch_text: convict.to_owned(),
            source: self.server_id.clone(),
            action,
            switch_group_name: switch_group_name.to_owned(),
            exceed_cause: exceed_cause.to_owned(),
        };
        // Add record without ID into watch list in this TiDB right now.
        self.add_watch_list(record.clone(), ttl, false);
        let mut queues = self.channels.lock();
        if queues.quarantine.len() < MAX_WATCH_RECORD_CHANNEL_SIZE {
            queues.quarantine.push_back(record);
            drop(queues);
            self.channels.readable.notify_all();
        }
        // A full channel drops the record (Go's TODO: warn).
    }

    /// Go `addWatchList`.
    fn add_watch_list(&self, record: QuarantineRecord, ttl: Duration, force: bool) {
        let key = record.get_record_key();
        // A pre-check: a key is rarely added twice.
        let item = self.get_watch_from_watch_list(&key);
        let lock = || self.query_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if force {
            let _guard = lock();
            if let Some(item) = item {
                // check the ID because of the earlier scan.
                if item.id == record.id {
                    return;
                }
                self.watch_list.delete(&key);
            }
            self.watch_list.set(&key, record, ttl);
            return;
        }
        match item {
            None => {
                let _guard = lock();
                // A stale record reads as absent, so absent means add.
                if self.watch_list.get(&key).is_none() {
                    self.watch_list.set(&key, record, ttl);
                } else {
                    self.channels.send_stale(record);
                }
            }
            // to replace the record without ID.
            Some(item) if item.id == 0 => {
                let _guard = lock();
                self.watch_list.set(&key, record, ttl);
            }
            // check the ID because of the earlier scan.
            Some(item) if item.id != record.id => self.channels.send_stale(record),
            Some(_) => {}
        }
    }

    /// Go `GetWatchList`: every watch item.
    pub fn get_watch_list(&self) -> Vec<QuarantineRecord> {
        self.watch_list.items()
    }

    fn get_watch_from_watch_list(&self, key: &str) -> Option<QuarantineRecord> {
        self.watch_list.get(key)
    }

    /// Go `markRunaway`.
    pub(crate) fn mark_runaway(
        &self,
        checker: &Checker,
        action: &str,
        match_type: &str,
        now: DateTime<Local>,
        exceed_cause: &str,
    ) {
        let record = Record {
            resource_group_name: checker.resource_group_name().to_owned(),
            start_time: now.with_timezone(&Utc),
            match_type: match_type.to_owned(),
            action: action.to_owned(),
            sample_text: checker.original_sql().to_owned(),
            sql_digest: checker.sql_digest().to_owned(),
            plan_digest: checker.plan_digest().to_owned(),
            source: self.server_id.clone(),
            exceed_cause: exceed_cause.to_owned(),
            // default value for Repeats
            repeats: 1,
        };
        let mut queues = self.channels.lock();
        if queues.runaway_queries.len() < MAX_WATCH_RECORD_CHANNEL_SIZE {
            queues.runaway_queries.push_back(record);
            drop(queues);
            self.channels.readable.notify_all();
        }
        // A full channel drops the record (Go's TODO: warn).
    }

    /// Go `examineWatchList`: whether the query is in the watch list, with
    /// the item's action, switch group and exceed cause.
    pub(crate) fn examine_watch_list(
        &self,
        resource_group_name: &str,
        convict: &str,
    ) -> Option<(rmpb::RunawayAction, String, String)> {
        let item = self.get_watch_from_watch_list(&format!("{resource_group_name}/{convict}"))?;
        Some((item.action, item.get_switch_group_name().to_owned(), item.get_exceed_cause().to_owned()))
    }

    /// Go `getActiveWatchCount`.
    pub fn get_active_watch_count(&self, name: &str) -> i64 {
        self.active_group.get_active_watch_count(name)
    }

    /// Go `Stop`: stops the watch list's cleaner.
    pub fn stop(&self) {
        self.watch_list.stop();
        self.channels.lock().exit = true;
        self.channels.readable.notify_all();
        self.channels.writable.notify_all();
    }

    /// Go `UpdateNewAndDoneWatch`: adds the new watch items and removes the
    /// done ones.
    ///
    /// # Errors
    /// Whatever reading the system tables reports.
    pub fn update_new_and_done_watch(&self) -> Result<(), String> {
        let mut state = self.runaway_syncer.mu.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        if let Some(last) = state.last_sync_time {
            Syncer::sync_interval_observe(now.duration_since(last).as_secs_f64());
        }
        state.last_sync_time = Some(now);
        let start = Instant::now();
        let result = self.do_sync(&mut state);
        Syncer::sync_duration_observe(start.elapsed().as_secs_f64());
        Syncer::sync_counter_inc(result.is_ok());
        result?;
        Syncer::checkpoint_gauges_set(&state);
        Ok(())
    }

    /// Go `doSync`: the sync of the watch and watch-done tables.
    fn do_sync(&self, state: &mut crate::syncer::SyncState) -> Result<(), String> {
        if !self.runaway_syncer.check_watch_table_exist() {
            return Ok(());
        }
        let records = self.runaway_syncer.scan_new_records_in_range(&mut state.new_watch_reader)?;
        for record in records {
            self.add_watch(record);
        }
        // The first sync skips the historical watch_done scan: the watch list
        // was just loaded from the watch table, which no longer holds the
        // records moved to watch_done. The overlap covers moves that commit
        // between the two snapshots.
        if state.deletion_watch_reader.check_point == null_time() {
            state.deletion_watch_reader.check_point = state.new_watch_reader.upper_bound
                - chrono::Duration::from_std(WATCH_SYNC_OVERLAP).expect("overlap fits");
        }
        if !self.runaway_syncer.check_watch_done_table_exist() {
            return Ok(());
        }
        let done_records = self.runaway_syncer.scan_new_records_in_range(&mut state.deletion_watch_reader)?;
        for record in done_records {
            self.remove_watch(&record);
        }
        Ok(())
    }

    /// Go `AddWatch`: adds a watch item from the system table.
    pub fn add_watch(&self, record: QuarantineRecord) {
        let ttl = if record.end_time == null_time() {
            Duration::ZERO
        } else {
            match (record.end_time - Utc::now()).to_std() {
                Ok(ttl) if !ttl.is_zero() => ttl,
                _ => {
                    self.channels.send_stale(record);
                    return;
                }
            }
        };
        // The manual record replaces the old record.
        let force = record.source == MANUAL_SOURCE;
        self.add_watch_list(record, ttl, force);
    }

    /// Go `removeWatch`: removes the item a done-watch row names, when the
    /// cached item is that record.
    fn remove_watch(&self, record: &QuarantineRecord) {
        let _guard = self.query_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = record.get_record_key();
        if self.get_watch_from_watch_list(&key).is_some_and(|item| item.id == record.id) {
            self.watch_list.delete(&key);
        }
    }

    /// Go `DeriveChecker`: the statement's checker, `None` when its group
    /// has neither runaway settings nor active watches, or it has no plan.
    pub fn derive_checker(
        self: &Arc<Self>,
        resource_group_name: &str,
        original_sql: &str,
        sql_digest: &str,
        plan_digest: &str,
        start_time: DateTime<Local>,
    ) -> Option<Arc<Checker>> {
        let group = match self.resource_group_ctl.get_resource_group(resource_group_name) {
            Ok(Some(group)) => group,
            Ok(None) => {
                tidb_logutil::bg_logger().warn("cannot setup up runaway checker", &[]);
                return None;
            }
            Err(error) => {
                tidb_logutil::bg_logger().warn(
                    "cannot setup up runaway checker",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error))],
                );
                return None;
            }
        };
        // Only check the normal statement.
        if plan_digest.is_empty() {
            return None;
        }
        if group.runaway_settings.is_none() && self.get_active_watch_count(resource_group_name) == 0 {
            return None;
        }
        tidb_metrics::server::TIDB_SERVER_QUERY_RUNAWAY_CHECK.with_label_values(&[resource_group_name, "hit", ""]).inc();
        Some(Arc::new(Checker::new(
            Arc::clone(self),
            resource_group_name,
            group.runaway_settings,
            original_sql,
            sql_digest,
            plan_digest,
            start_time,
        )))
    }
}

/// Go `flushThreshold`: the record count that triggers a flush.
fn flush_threshold() -> usize {
    MAX_WATCH_RECORD_CHANNEL_SIZE / 2
}

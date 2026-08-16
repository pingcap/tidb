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

//! Transcreation of Go `pkg/timer/api/mem_store.go`: the in-memory timer store
//! and the in-memory watch-event notifier.

use std::collections::HashMap;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;

use tidb_util::timeutil::{parse_time_zone, system_location, TimeZone};

use crate::error::{Result, TimerError};
use crate::store::{
    Cond, Context, TimerStore, TimerStoreCore, TimerUpdate, TimerWatchEventNotifier,
    WatchTimerChan, WatchTimerEvent, WatchTimerEventType, WatchTimerResponse,
};
use crate::timer::{SchedEventStatus, TimerRecord};
use crate::uuid::new_uuid_hex;

/// Go `memStoreWatcher`.
struct MemStoreWatcher {
    ctx: Context,
    sender: SyncSender<WatchTimerResponse>,
}

#[derive(Default)]
struct MemoryStoreState {
    /// Go `namespaces`: namespace -> key -> record.
    namespaces: HashMap<String, HashMap<String, Arc<TimerRecord>>>,
    /// Go `id2Timers`.
    id_to_timers: HashMap<String, Arc<TimerRecord>>,
}

/// Go `memoryStoreCore`.
pub struct MemoryStoreCore {
    state: RwLock<MemoryStoreState>,
    notifier: Arc<dyn TimerWatchEventNotifier>,
}

impl MemoryStoreCore {
    /// The core behind Go's `NewMemoryTimerStore`.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(MemoryStoreState::default()),
            notifier: Arc::new(MemTimerWatchEventNotifier::new()),
        }
    }
}

impl Default for MemoryStoreCore {
    fn default() -> Self {
        Self::new()
    }
}

/// Go `NewMemoryTimerStore`.
pub fn new_memory_timer_store() -> TimerStore {
    TimerStore::new(Arc::new(MemoryStoreCore::new()))
}

impl TimerStoreCore for MemoryStoreCore {
    fn create(&self, _ctx: &Context, record: &TimerRecord) -> Result<String> {
        if !record.id.is_empty() {
            return Err(TimerError::message(
                "ID should not be specified when create record",
            ));
        }

        if record.version != 0 {
            return Err(TimerError::message(
                "Version should not be specified when create record",
            ));
        }

        if !record.create_time.is_zero() {
            return Err(TimerError::message(
                "CreateTime should not be specified when create record",
            ));
        }

        record.validate()?;

        let mut state = self.state.write().unwrap();

        let mut record = record.clone_record();
        record.id = new_uuid_hex();
        record.location = Some(get_mem_store_time_zone_loc(&record.spec.time_zone));
        record.version = 1;
        record.create_time = crate::go_time::GoTime::now();

        if record.event_status.as_str().is_empty() {
            record.event_status = SchedEventStatus::idle();
        }

        normalize_time_fields(&mut record);

        if state.id_to_timers.contains_key(&record.id) {
            return Err(TimerError::TimerExists);
        }

        if let Some(namespace) = state.namespaces.get(&record.spec.namespace) {
            if namespace.contains_key(&record.spec.key) {
                return Err(TimerError::TimerExists);
            }
        }

        let id = record.id.clone();
        let namespace = record.spec.namespace.clone();
        let key = record.spec.key.clone();
        let shared = Arc::new(record);
        state.id_to_timers.insert(id.clone(), Arc::clone(&shared));
        state
            .namespaces
            .entry(namespace)
            .or_default()
            .insert(key, shared);
        drop(state);

        self.notifier.notify(WatchTimerEventType::Create, &id);
        Ok(id)
    }

    fn list(&self, _ctx: &Context, cond: Option<&dyn Cond>) -> Result<Vec<TimerRecord>> {
        let state = self.state.read().unwrap();
        let mut result = Vec::with_capacity(1);
        for namespace in state.namespaces.values() {
            for timer in namespace.values() {
                if cond.is_none_or(|cond| cond.match_record(timer)) {
                    result.push(timer.clone_record());
                }
            }
        }
        Ok(result)
    }

    fn update(&self, _ctx: &Context, timer_id: &str, update: &TimerUpdate) -> Result<()> {
        let mut state = self.state.write().unwrap();

        let record = state
            .id_to_timers
            .get(timer_id)
            .ok_or(TimerError::TimerNotExist)?;
        let namespace = record.spec.namespace.clone();
        let key = record.spec.key.clone();

        let mut new_record = update.apply(record)?;

        normalize_time_fields(&mut new_record);
        new_record.validate()?;

        new_record.version += 1;
        let shared = Arc::new(new_record);
        state
            .id_to_timers
            .insert(timer_id.to_string(), Arc::clone(&shared));
        if let Some(namespace) = state.namespaces.get_mut(&namespace) {
            namespace.insert(key, shared);
        }
        drop(state);

        self.notifier.notify(WatchTimerEventType::Update, timer_id);
        Ok(())
    }

    fn delete(&self, _ctx: &Context, timer_id: &str) -> Result<bool> {
        let mut state = self.state.write().unwrap();
        let Some(record) = state.id_to_timers.remove(timer_id) else {
            return Ok(false);
        };

        if let Some(namespace) = state.namespaces.get_mut(&record.spec.namespace) {
            namespace.remove(&record.spec.key);
            if namespace.is_empty() {
                state.namespaces.remove(&record.spec.namespace);
            }
        }
        drop(state);

        self.notifier.notify(WatchTimerEventType::Delete, timer_id);
        Ok(true)
    }

    fn watch_supported(&self) -> bool {
        true
    }

    fn watch(&self, ctx: &Context) -> WatchTimerChan {
        self.notifier.watch(ctx)
    }

    fn close(&self) {
        self.notifier.close();
    }
}

#[derive(Default)]
struct NotifierState {
    /// `None` once `Close` has run, standing in for Go's nil'ed `cancel`.
    open: bool,
    watchers: Vec<MemStoreWatcher>,
    pending: Vec<JoinHandle<()>>,
}

/// Go `memTimerWatchEventNotifier`.
///
/// Go relays each watcher's 8-slot buffered channel into an unbuffered one
/// through a goroutine so it can `select` on both the notifier's and the
/// watcher's contexts. Rust's `mpsc` receiver already reports closure when the
/// sender drops, so the relay hop is unnecessary and the watcher reads the
/// 8-slot buffer directly; the only observable difference is that a consumer
/// can now be up to eight responses behind without a producer blocking, where
/// Go allowed nine.
pub struct MemTimerWatchEventNotifier {
    state: Mutex<NotifierState>,
}

impl MemTimerWatchEventNotifier {
    /// Go `NewMemTimerWatchEventNotifier`'s concrete value.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(NotifierState {
                open: true,
                watchers: Vec::with_capacity(8),
                pending: Vec::new(),
            }),
        }
    }
}

impl Default for MemTimerWatchEventNotifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Go `NewMemTimerWatchEventNotifier`.
pub fn new_mem_timer_watch_event_notifier() -> Arc<dyn TimerWatchEventNotifier> {
    Arc::new(MemTimerWatchEventNotifier::new())
}

impl TimerWatchEventNotifier for MemTimerWatchEventNotifier {
    fn watch(&self, ctx: &Context) -> WatchTimerChan {
        let mut state = self.state.lock().unwrap();
        let (sender, receiver) = sync_channel(8);
        if !state.open {
            // Go closes the channel immediately; dropping the sender is the
            // Rust spelling of a closed channel.
            return receiver;
        }
        state.watchers.push(MemStoreWatcher {
            ctx: ctx.clone(),
            sender,
        });
        receiver
    }

    fn notify(&self, tp: WatchTimerEventType, timer_id: &str) {
        let mut state = self.state.lock().unwrap();
        if !state.open {
            return;
        }

        let response = WatchTimerResponse {
            events: vec![WatchTimerEvent {
                tp,
                timer_id: timer_id.to_string(),
            }],
        };

        let mut spawned = Vec::new();
        state.watchers.retain(|watcher| {
            if watcher.ctx.is_done() {
                return false;
            }
            match watcher.sender.try_send(response.clone()) {
                Ok(()) => true,
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => false,
                Err(std::sync::mpsc::TrySendError::Full(response)) => {
                    // Go spawns a goroutine that blocks until either the
                    // watcher accepts the response or a context is cancelled.
                    let sender = watcher.sender.clone();
                    let ctx = watcher.ctx.clone();
                    spawned.push(std::thread::spawn(move || {
                        while !ctx.is_done() {
                            match sender.try_send(response.clone()) {
                                Ok(()) | Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                                    return
                                }
                                Err(std::sync::mpsc::TrySendError::Full(_)) => {
                                    std::thread::sleep(std::time::Duration::from_millis(1));
                                }
                            }
                        }
                    }));
                    true
                }
            }
        });
        state.pending.append(&mut spawned);
    }

    fn close(&self) {
        let pending = {
            let mut state = self.state.lock().unwrap();
            state.open = false;
            state.watchers.clear();
            std::mem::take(&mut state.pending)
        };
        // Go's `wg.Wait()`.
        for handle in pending {
            let _ = handle.join();
        }
    }
}

/// Go `getMemStoreTimeZoneLoc`.
pub fn get_mem_store_time_zone_loc(time_zone: &str) -> TimeZone {
    if time_zone.is_empty() {
        return system_location();
    }

    match parse_time_zone(time_zone) {
        Ok(location) => location,
        Err(_) => system_location(),
    }
}

/// Go `normalizeTimeFields`.
pub fn normalize_time_fields(record: &mut TimerRecord) {
    let Some(location) = record.location.clone() else {
        return;
    };

    if !record.spec.watermark.is_zero() {
        record.spec.watermark = record.spec.watermark.in_location(&location);
    }

    if !record.event_start.is_zero() {
        record.event_start = record.event_start.in_location(&location);
    }

    if !record.create_time.is_zero() {
        record.create_time = record.create_time.in_location(&location);
    }
}

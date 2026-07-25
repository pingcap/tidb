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

//! Transcreation of Go `pkg/util/memory/action.go`.

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, LazyLock, Mutex};

use tidb_error::mysql::FormatArg;
use tidb_error::terror::TerrorError;
use tidb_error::tidb::errcode;

use super::Tracker;
use crate::sqlkiller::{KillSignal, SqlKiller};

/// A shared action handle (Go's `ActionOnExceed` interface value).
pub type ArcAction = Arc<dyn ActionOnExceed + Send + Sync>;

/// Default OOM action priorities (Go's iota block).
pub const DEF_PANIC_PRIORITY: i64 = 0;
/// `DefLogPriority`.
pub const DEF_LOG_PRIORITY: i64 = 1;
/// `DefSpillPriority`.
pub const DEF_SPILL_PRIORITY: i64 = 2;
/// `DefCursorFetchSpillPriority`: higher than a normal spill because it can
/// release much more memory later at lower execution cost.
pub const DEF_CURSOR_FETCH_SPILL_PRIORITY: i64 = 3;
/// `DefRateLimitPriority`.
pub const DEF_RATE_LIMIT_PRIORITY: i64 = 4;

/// `errMemExceedThreshold` (Go `action.go`).
static ERR_MEM_EXCEED_THRESHOLD: LazyLock<TerrorError> =
    LazyLock::new(|| crate::dbterror::CLASS_UTIL.new_std(errcode::ErrMemExceedThreshold));

/// The action taken when memory usage exceeds a quota (Go `ActionOnExceed`).
/// Implementors must be thread-safe.
pub trait ActionOnExceed {
    /// Called when the corresponding tracker exceeds its quota.
    fn action(&self, t: &Arc<Tracker>);
    /// Sets the fallback triggered once this action has already acted.
    fn set_fallback(&self, a: Option<ArcAction>);
    /// Gets the fallback action, skipping finished links (Go `GetFallback`).
    fn get_fallback(&self) -> Option<ArcAction>;
    /// The action's priority.
    fn get_priority(&self) -> i64;
    /// Marks the action finished.
    fn set_finished(&self);
    /// Whether the action is finished.
    fn is_finished(&self) -> bool;
}

/// Manages the fallback chain for every action (Go `BaseOOMAction`).
#[derive(Default)]
pub struct BaseOomAction {
    fallback: Mutex<Option<ArcAction>>,
    finished: AtomicBool,
}

impl BaseOomAction {
    /// Go `SetFallback`.
    pub fn set_fallback(&self, a: Option<ArcAction>) {
        *self.fallback.lock().unwrap() = a;
    }

    /// Go `SetFinished`.
    pub fn set_finished(&self) {
        self.finished.store(true, SeqCst);
    }

    /// Go `IsFinished`.
    pub fn is_finished(&self) -> bool {
        self.finished.load(SeqCst)
    }

    /// Go `GetFallback`: drops finished links before returning.
    pub fn get_fallback(&self) -> Option<ArcAction> {
        let mut fallback = self.fallback.lock().unwrap();
        while let Some(a) = fallback.clone() {
            if !a.is_finished() {
                return Some(a);
            }
            *fallback = a.get_fallback();
        }
        None
    }

    /// Go `TriggerFallBackAction`.
    pub fn trigger_fallback_action(&self, tracker: &Arc<Tracker>) {
        if let Some(fallback) = self.get_fallback() {
            fallback.action(tracker);
        }
    }
}

/// Logs a warning only once when memory usage exceeds the quota (Go
/// `LogOnExceed`).
#[derive(Default)]
pub struct LogOnExceed {
    base: BaseOomAction,
    /// The connection ID passed to the log hook.
    pub conn_id: u64,
    state: Mutex<LogState>,
}

#[derive(Default)]
struct LogState {
    acted: bool,
    log_hook: Option<Box<dyn Fn(u64) + Send + Sync>>,
}

impl LogOnExceed {
    /// Sets the log hook.
    pub fn set_log_hook(&self, hook: Box<dyn Fn(u64) + Send + Sync>) {
        self.state.lock().unwrap().log_hook = Some(hook);
    }
}

impl ActionOnExceed for LogOnExceed {
    fn action(&self, t: &Arc<Tracker>) {
        let mut state = self.state.lock().unwrap();
        if !state.acted {
            state.acted = true;
            match &state.log_hook {
                None => tracing::warn!(
                    error = %mem_exceed_error(t),
                    "memory exceeds quota"
                ),
                Some(hook) => hook(self.conn_id),
            }
        }
    }

    fn set_fallback(&self, a: Option<ArcAction>) {
        self.base.set_fallback(a);
    }
    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }
    fn get_priority(&self) -> i64 {
        DEF_LOG_PRIORITY
    }
    fn set_finished(&self) {
        self.base.set_finished();
    }
    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

/// Panics (with the kill error) when memory usage exceeds the quota (Go
/// `PanicOnExceed`).
pub struct PanicOnExceed {
    base: BaseOomAction,
    /// The query killer receiving `QueryMemoryExceeded`.
    pub killer: Arc<SqlKiller>,
    /// The connection ID passed to the log hook.
    pub conn_id: u64,
    state: Mutex<LogState>,
}

impl PanicOnExceed {
    /// Creates the action around a killer.
    pub fn new(killer: Arc<SqlKiller>) -> Self {
        PanicOnExceed {
            base: BaseOomAction::default(),
            killer,
            conn_id: 0,
            state: Mutex::new(LogState::default()),
        }
    }

    /// Sets the log hook.
    pub fn set_log_hook(&self, hook: Box<dyn Fn(u64) + Send + Sync>) {
        self.state.lock().unwrap().log_hook = Some(hook);
    }
}

impl ActionOnExceed for PanicOnExceed {
    fn action(&self, t: &Arc<Tracker>) {
        {
            let mut state = self.state.lock().unwrap();
            if !state.acted {
                match &state.log_hook {
                    None => tracing::warn!(
                        conn = t.session_id(),
                        error = %mem_exceed_error(t),
                        "memory exceeds quota"
                    ),
                    Some(hook) => hook(self.conn_id),
                }
            }
            state.acted = true;
        }
        self.killer
            .send_kill_signal(KillSignal::QueryMemoryExceeded);
        if let Some(err) = self.killer.handle_signal() {
            std::panic::panic_any(err.to_string());
        }
    }

    fn set_fallback(&self, a: Option<ArcAction>) {
        self.base.set_fallback(a);
    }
    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }
    fn get_priority(&self) -> i64 {
        DEF_PANIC_PRIORITY
    }
    fn set_finished(&self) {
        self.base.set_finished();
    }
    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

/// Formats Go's `errMemExceedThreshold.GenWithStackByArgs(label, consumed,
/// limit, tree)`.
fn mem_exceed_error(t: &Arc<Tracker>) -> TerrorError {
    let template = ERR_MEM_EXCEED_THRESHOLD.message().to_string();
    ERR_MEM_EXCEED_THRESHOLD.fast_generate(
        &template,
        &[
            FormatArg::from(t.label()),
            FormatArg::from(t.bytes_consumed()),
            FormatArg::from(t.get_bytes_limit()),
            FormatArg::from(t.tree_string().as_str()),
        ],
    )
}
